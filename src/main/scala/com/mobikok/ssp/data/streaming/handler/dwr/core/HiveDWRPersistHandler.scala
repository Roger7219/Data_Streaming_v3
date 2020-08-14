package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.HiveTransactionCookie
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.module.support.TimeGranularity
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConversions._

class HiveDWRPersistHandler(dwrTable: String, subDwrTable: String, val subDwrTableGroupByFields: Array[String], where: String, timeGranularity: TimeGranularity) extends Handler {

  def this(dwrTable: String, groupByFields: Array[String], where: String, timeGranularity: TimeGranularity){
    this(dwrTable, dwrTable, groupByFields, where, timeGranularity)
  }

  var cookie: TransactionCookie = _
  var needCounterMapFields:Set[String] = _

  var aggExprsAlias: List[String] = _
  var unionAggExprsAndAlias: List[Column] = _
  var overwriteAggFields: Set[String] = _
  var groupByExprsAlias: Array[String] = _

  var partitionFields: Array[String] = _

  // [(dwrSubTable, subTableGroupByFields, where, timeGranularity)]
  var subTables: List[(String, Array[String], String, String)] = _
  @volatile var currentBatchCookies: util.List[TransactionCookie] = _

  override def init(moduleName: String, handlerName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, handlerName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, messageClient, moduleTracer)
    // 异步
    isAsynchronous = true

    aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
//
    unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    overwriteAggFields = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => if(x.hasPath("overwrite") && x.getBoolean("overwrite")) x.getString("as") else null
    }.filter(_ != null).toSet

    groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

    partitionFields = Array("l_time", "b_date", "b_time"/*, "b_version"*/)

    // 如果分表不是自身（dwr.table定义的）
    if(!subDwrTable.equals(dwrTable)) {
      hiveClient.createTableIfNotExists(subDwrTable, dwrTable)
    }

  }

  override def doHandle(persistenceDwr: DataFrame): DataFrame = {

    currentBatchCookies = new util.ArrayList[TransactionCookie]()

    val ts = partitionFields
    val ps = persistenceDwr
      .dropDuplicates(ts)
      .collect()
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }

    var resultDF: DataFrame = persistenceDwr

    if(StringUtil.notEmpty(where)) {
      resultDF = resultDF.where(where)
    }

    // 只统计分表指定的维度字段, 而非统计所有维度字段
    if(!subDwrTableGroupByFields.contains("*")){

      // Group by fields会统一转成小写比较，使配置大小写不敏感
      val subTableAllGroupByFieldsLowerCase = (subDwrTableGroupByFields ++ partitionFields).map(_.toLowerCase)
      val subTableAllGroupByFields = (subDwrTableGroupByFields ++ partitionFields).map(forColumn(_, timeGranularity))

      resultDF = resultDF
        .groupBy(subTableAllGroupByFields:_*)
        .agg(unionAggExprsAndAlias.head, unionAggExprsAndAlias.tail: _*)
        .select(
          persistenceDwr.schema.fields.map { f =>
            var c: Column = null
            val fLowerCase = f.name.toLowerCase

            // 是否子表需统计的字段
            if(subTableAllGroupByFieldsLowerCase.contains(fLowerCase)){
              c = forColumn(f.name, timeGranularity)
            }else {
              c = expr("null").cast(f.dataType).as(f.name)
            }

            c
          }: _*
        )
    }

    currentBatchCookies.add(
      hiveClient.overwriteUnionSum(
        transactionManager.getCurrentTransactionParentId(),
        subDwrTable,
        resultDF,
        aggExprsAlias,
        unionAggExprsAndAlias,
        overwriteAggFields,
        groupByExprsAlias,
        ps,
        null,
        partitionFields.head,
        partitionFields.tail:_*
      )
    )

    transactionManager.collectTransactionCookies(hiveClient, currentBatchCookies)

    persistenceDwr
  }

  override def doCommit(): Unit = {

    currentBatchCookies.par.foreach{cookie=>

      hiveClient.commit(cookie)

      // push message
      val dwrT = cookie.asInstanceOf[HiveTransactionCookie]
      val topic = dwrT.targetTable
      if (dwrT != null && dwrT.partitions != null && dwrT.partitions.nonEmpty) {
        val key = OM.toJOSN(dwrT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
        messageClient.push(PushReq(topic, key))
        LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")
      } else {
        LOG.warn(s"MessageClient dwr no hive partitions to push", s"topic: $topic")
      }
    }
    currentBatchCookies = null
  }


  override def doClean(): Unit = {
    transactionManager.cleanLastTransactionCookie(hiveClient)
  }


  def forColumn(fieldName: String, timeGranularity: TimeGranularity): Column={
    if(fieldName.equals("b_time")){
      timeGranularity match {
        case TimeGranularity.Day     => expr(s"from_unixtime(unix_timestamp(b_time), 'yyyy-MM-dd 00:00:00')").as("b_time")
        case TimeGranularity.Month   => expr(s"from_unixtime(unix_timestamp(b_time), 'yyyy-MM-01 00:00:00')").as("b_time")
        case TimeGranularity.Default => col(fieldName)
      }
    }
    else if(fieldName.equals("l_time")){
      timeGranularity match {
        case TimeGranularity.Day     => expr(s"from_unixtime(unix_timestamp(l_time), 'yyyy-MM-dd 00:00:00')").as("l_time")
        case TimeGranularity.Month   => expr(s"from_unixtime(unix_timestamp(l_time), 'yyyy-MM-01 00:00:00')").as("l_time")
        case TimeGranularity.Default => col(fieldName)
      }
    }
    else if(fieldName.equals("b_date")) {
      timeGranularity match {
        case TimeGranularity.Day     => col(fieldName)
        case TimeGranularity.Month   => expr(s"from_unixtime(unix_timestamp(b_date), 'yyyy-MM-01')").as("b_date")
        case TimeGranularity.Default => col(fieldName)
      }
    }else {
      col(fieldName)
    }
  }
}






















//    var result = Array[TransactionCookie]()
//
//    val mixTransactionManager = transactionManager.asInstanceOf[TransactionManager]
//    if (mixTransactionManager.needRealTransactionalAction()) {
//      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
//      result = needCleans.toArray
//      batchTransactionCookiesCache.removeAll(needCleans)
//    }
//    hiveClient.clean(result:_*)