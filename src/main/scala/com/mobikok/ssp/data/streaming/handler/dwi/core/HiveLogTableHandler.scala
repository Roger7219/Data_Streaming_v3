package com.mobikok.ssp.data.streaming.handler.dwi.core

import java.util

import com.mobikok.ssp.data.streaming.OptimizedMixApp.allModulesConfig
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{expr, lit, sum}

import scala.collection.JavaConversions._

class HiveLogTableHandler extends Handler {

  //  val COOKIE_KIND_DWI_T = "dwiT"

  val logTableColumnCount: String = "count";
  val logTableColumnTableName: String = "table_name";
  val logTableColumnFieldName: String = "field_name";
  val logTableColumnFieldValue: String = "field_value";
  val configFiled: String = "others";
  var logtable: String = "nadx_log_table"
  // 有配置表示enable
  var table: String = _
  var cookie: TransactionCookie = _

  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)
    table = globalConfig.getString(s"modules.$moduleName.dwi.table")
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies: _*)
  }


  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {
    //    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwi.partition.fields")
    var fields: List[Config] = null
    if(allModulesConfig.hasPath(s"modules.$moduleName.dwr.groupby.fields")){
      // 排除dwr.groupby.fields中不需要统计的字段
      fields = allModulesConfig
        .getConfigList(s"modules.$moduleName.dwr.groupby.fields")
        .filter(x => if(x.hasPath(configFiled)) x.getBoolean(configFiled) else false).toList
    }


    if(fields !=null && fields.size > 0) {
      var result: DataFrame = null;

      val aggExprsAlias = List(logTableColumnCount)

      val unionAggExprsAndAlias = List(expr("sum(count)").as(logTableColumnCount))

      val overwriteAggFields = Set(logTableColumnCount)

      val groupByExprsAlias = Array(logTableColumnFieldName, logTableColumnFieldValue, logTableColumnTableName)

      val partitionFields = Array("l_time", "b_date", "b_time", "b_version", logTableColumnFieldName)
      fields.foreach(filed => {

        var partitionFieldsTmp = partitionFields
        //分区字段 和回滚分区不一致  特此处理
        partitionFieldsTmp = partitionFieldsTmp.filter(f => !f.equals(logTableColumnFieldName))
        //帥選列
        var persistenceDwr_ = newDwi.select(filed.getString("as"), partitionFieldsTmp: _*)
          //group by
          .groupBy(filed.getString("as"), partitionFieldsTmp: _*)
          //計數
          .agg(sum(lit(1)).as(logTableColumnCount))
          //增加字段
          .withColumn(logTableColumnTableName, expr(s"'${table}'"))
          //增加字段
          .withColumn(logTableColumnFieldName, expr(s"'${filed.getString("as")}'"))
          //增加字段
          .withColumn(logTableColumnFieldValue, expr(filed.getString("as")))
          //刪除無用字段
          .drop(filed.getString("as"))

        if (result == null) {
          //初次賦值
          result = persistenceDwr_;
        } else {
          //非初次 匯聚
          result = result.unionAll(persistenceDwr_)
        }
      })

      if(isOverwriteFixedLTime) {
        //寫入數據
        cookie = hiveClient.overwriteUnionSum(
          transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
          logtable,
          result,
          aggExprsAlias,
          unionAggExprsAndAlias,
          overwriteAggFields,
          groupByExprsAlias,
          null,
          partitionFields.head,
          partitionFields.tail:_*
        )
      } else {
        //寫入數據
        cookie = hiveClient.overwriteUnionSum(
          transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
          logtable,
          result,
          aggExprsAlias,
          unionAggExprsAndAlias,
          overwriteAggFields,
          groupByExprsAlias,
          null,
          partitionFields.head,
          partitionFields.tail:_*
        )
      }
      batchTransactionCookiesCache.add(cookie)
    }



    LOG.warn("hiveClient write dwiTable completed", cookie)
    (newDwi, Array(cookie))
  }

  override def commit(c: TransactionCookie): Unit = {
    hiveClient.commit(cookie)

    // push message
    val dwiT = cookie.asInstanceOf[HiveTransactionCookie]
    var topic = dwiT.targetTable
    if (dwiT != null && dwiT.partitions != null && dwiT.partitions.nonEmpty) {
      val key = OM.toJOSN(dwiT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      MC.push(PushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

      topic = moduleName
      ThreadPool.LOCK.synchronized {
        MC.push(PushReq(topic, key))
      }
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

    } else {
      LOG.warn(s"MessageClient dwi no hive partitions to push", s"topic: $topic")
    }
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    var result = Array[TransactionCookie]()

    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
    if (mixTransactionManager.needTransactionalAction()) {
      //      val needCleans = batchTransactionCookiesCache.filter{ x => !x.parentId.equals(mixTransactionManager.getCurrentTransactionParentId())}
      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
      batchTransactionCookiesCache.removeAll(needCleans)
      result = needCleans.toArray
    }
    hiveClient.clean(result:_*)
  }

}