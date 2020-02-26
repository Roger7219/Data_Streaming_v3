package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConversions._

class HiveDWRPersistHandlerV2 extends Handler with Persistence {

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())
//  val COOKIE_KIND_DWR_T = "dwrT"

  var table: String = _
  var cookie: TransactionCookie = _
  var needCounterMapFields:Set[String] = _

  var aggExprsAlias: List[String] = _
  var unionAggExprsAndAlias: List[Column] = _
  var overwriteAggFields: Set[String] = _
  var groupByExprsAlias: Array[String] = _

  var counterAliasMaps: Map[String, String] = _
  // gsExpr         : "a,b"
  // whereTableAlias: "`{a,b}.count>10`"
  // where          : "`{a,b}.count>10`.count>10"
  var counterWhereExprs: Set[(String, String, String)] = _
  // <`{a,b}.count>10`, `{a,b}.count>10`.count>10">
  var counterAliasReplacedMap: Map[String, String] = _

//  var counterTablePrefix = "___"
  var partitionFields: Array[String] = _

  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  var subversionTables: List[(String, Array[String], String)] = _
  @volatile var currentBatchCookies: util.List[TransactionCookie] = _

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, exprString: String, as: String): Unit = {
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, exprString, as)
    // 异步
    isAsynchronous = true
    table = globalConfig.getString(s"modules.$moduleName.dwr.table")

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

    partitionFields = Array("l_time", "b_date", "b_time", "b_version")

    counterAliasMaps = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => if(x.hasPath("map")) x.getString("as") -> x.getString("map") else null
    }.filter(_ != null).toMap

    // gsExpr    : "a,b"
    // tableAlias: "`{a,b}.count>10`"
    // where     : "`{a,b}.count>10`.count>10"
    counterWhereExprs = counterAliasMaps
      // [${a,b}.count>10, ${a,b}.count>20]
      .map{case(_, map)=> StringUtil.getMatcher("(\\{.+?\\}.+?)\\s*(?:,|[oO][rR]|[aA][nN][dD])", StringUtil.standardizedSQL(map))}
      .flatMap{x=>x}
      .toSet[String]
      .map{x=>(
        StringUtil.getFirstMatcher("\\{(.+?)\\}\\.", x),
        x,
        x.replaceAll("\\{(.+?)}", "`" + x + "`")
      )}

    counterAliasReplacedMap = counterAliasMaps.map{case(as, map)=>
      as-> StringUtil.standardizedSQL(map).replaceAll("(\\{.+?}(.+?))(\\s*)(,|[oO][rR]|[aA][nN][dD])", "`$1`$2$3$4")
    }

    // 判断需要统计的维度字段
    needCounterMapFields = counterAliasMaps.map{ case(_, map)=>
      StringUtil.getMatcher("\\$(.+?)\\.", map)
    }.flatMap{x=>x}.toSet

    val SUBVERSION_DEFAULT = "0"
    subversionTables = if(globalConfig.hasPath(s"modules.$moduleName.dwr.tables")) {
      val ts = s"modules.$moduleName.dwr.tables"
      globalConfig.getConfig(ts).root().map { x =>
        (
          if (SUBVERSION_DEFAULT.equals(x._1)) table else s"${table}_${x._1}",
          if(globalConfig.hasPath(s"$ts.${x._1}.select"))
            globalConfig.getString(s"$ts.${x._1}.select").split(",").map(_.trim).filter(StringUtil.notEmpty(_))
          else null,
          if(globalConfig.hasPath(s"$ts.${x._1}.where"))
            globalConfig.getString(s"$ts.${x._1}.where")
          else null
        )
      }.toList

    }else {
      List((
        table,
        null.asInstanceOf[Array[String]],
        null.asInstanceOf[String]
      ))
    }

    // 确保创建了对应的表
    subversionTables.foreach{ case(dwrSubversionTable, groupByFields, where)=>
      // subversion=0的dwr表名无子版本后缀，因此有table==dwrSubversionTable的情况
      if(dwrSubversionTable != table) {
        hiveClient.createTableIfNotExists(dwrSubversionTable, table)
//        sql(s"create table if not exists $dwrSubversionTable like $table")
      }
    }

  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {

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
//    moduleTracer.trace("        count partitions")

    subversionTables.par.foreach{case(dwrSubversionTable, groupByFields, where)=>

      var resultDF: DataFrame = persistenceDwr

      if(StringUtil.notEmpty(where)) {
        resultDF = resultDF.where(where)
      }

      if(groupByFields != null && groupByFields.length > 0) {

        resultDF = resultDF
          .groupBy((groupByFields ++ partitionFields).map(col(_)):_*)
          .agg(unionAggExprsAndAlias.head, unionAggExprsAndAlias.tail: _*)
          .select(
            persistenceDwr.schema.fields.map { f =>
              // 是维度字段 且 无需单维度统计的字段 置为null
              if(groupByExprsAlias.contains(f.name) && !groupByFields.contains(f.name))
                expr("null").cast(f.dataType).as(f.name)
              else
                col(f.name)
            }: _*
          )
      }

      currentBatchCookies.add(
        hiveClient.overwriteUnionSum(
          transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
          dwrSubversionTable,
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

      batchTransactionCookiesCache.addAll(currentBatchCookies)
    }

    persistenceDwr
  }

  override def prepare(dwi: DataFrame): DataFrame = {
    dwi
  }

  override def commit(c: TransactionCookie): Unit = {

    currentBatchCookies.par.foreach{cookie=>

      hiveClient.commit(cookie)

      // push message
      val dwrT = cookie.asInstanceOf[HiveTransactionCookie]
      val topic = dwrT.targetTable
      if (dwrT != null && dwrT.partitions != null && dwrT.partitions.nonEmpty) {
        val key = OM.toJOSN(dwrT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
        ThreadPool.LOCK.synchronized {
          MC.push(PushReq(topic, key))
        }
        //      messageClient.pushMessage(new MessagePushReq(topic, key))
        LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")
      } else {
        LOG.warn(s"MessageClient dwr no hive partitions to push", s"topic: $topic")
      }
    }
    currentBatchCookies = null
  }


  override def clean(cookies: TransactionCookie*): Unit = {
    var result = Array[TransactionCookie]()

    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
    if (mixTransactionManager.needTransactionalAction()) {
      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
      result = needCleans.toArray
      batchTransactionCookiesCache.removeAll(needCleans)
    }
    hiveClient.clean(result:_*)
  }
}
