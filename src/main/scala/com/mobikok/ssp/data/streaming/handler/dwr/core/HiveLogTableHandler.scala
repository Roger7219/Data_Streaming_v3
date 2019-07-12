package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util

import com.mobikok.ssp.data.streaming.OptimizedMixApp.allModulesConfig
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{count, expr, lit, sum}

import scala.collection.JavaConversions._

class HiveLogTableHandler extends Handler with Persistence {

  val logTableColumnCount: String = "count";
  val logTableColumnFieldName: String = "field_name";
  val logTableColumnFieldValue: String = "field_value";
  val configFiled: String = "others";
  var table: String = "nadx_log_table"
  var cookie: TransactionCookie = _

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())
  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, expr: String, as: String): Unit = {
    // 默认为true 遵从配置设置
    isAsynchronous = true
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, expr, as)
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {
    LOG.warn(s"HiveLogTableHandler start")
    var fields: List[Config] = null
    if(allModulesConfig.hasPath(s"modules.$moduleName.dwr.groupby.fields")){
      // 排除dwr.groupby.fields中不需要统计的字段
      fields = allModulesConfig
        .getConfigList(s"modules.$moduleName.dwr.groupby.fields")
          .filter(x => if(x.hasPath(configFiled)) x.getBoolean(configFiled) else false).toList
    }


    if(fields !=null && fields.size > 0){
      var result : DataFrame = null;

      val aggExprsAlias = List(logTableColumnCount)

      val unionAggExprsAndAlias = List(expr("sum(count)").as(logTableColumnCount))

      val overwriteAggFields = Set(logTableColumnCount)

      val groupByExprsAlias = Array(logTableColumnFieldName,logTableColumnFieldValue)

      val partitionFields = Array("l_time", "b_date", "b_time", "b_version")
      fields.foreach(filed =>{

        //帥選列
      var persistenceDwr_ = persistenceDwr.select(filed.getString("as"),partitionFields:_*)
        //group by
        .groupBy(filed.getString("as"),partitionFields:_*)
        //計數
        .agg(count(lit(1)).as(logTableColumnCount))
        //增加字段
        .withColumn(logTableColumnFieldName,expr(s"'${filed.getString("as")}'") )
        //增加字段
        .withColumn(logTableColumnFieldValue,expr(filed.getString("as")) )
        //刪除無用字段
        .drop(filed.getString("as"))

        if(result == null){
          //初次賦值
          result = persistenceDwr_;
        }else{
          //非初次 匯聚
          result = result.union(persistenceDwr_)
        }
      })

      //寫入數據
      cookie = hiveClient.overwriteUnionSum(
        transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
        table,
        result,
        aggExprsAlias,
        unionAggExprsAndAlias,
        overwriteAggFields,
        groupByExprsAlias,
        partitionFields.head,
        partitionFields.tail:_*
      )
      batchTransactionCookiesCache.add(cookie)

    }
    LOG.warn(s"HiveLogTableHandler end")
    persistenceDwr
  }

  override def commit(c: TransactionCookie): Unit = {
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
