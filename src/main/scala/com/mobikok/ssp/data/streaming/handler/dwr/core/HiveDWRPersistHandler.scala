package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConversions._

class HiveDWRPersistHandler extends Handler with Persistence {

//  val COOKIE_KIND_DWR_T = "dwrT"

  var table: String = _
  var cookie: TransactionCookie = _

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())
  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, expr: String, as: String): Unit = {
    // 默认为true 遵从配置设置
    isAsynchronous = true
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, expr, as)
//    table = handlerConfig.getString("table")
    table = globalConfig.getString(s"modules.$moduleName.dwr.table")
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {
    val aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList

    val unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    val groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwr.partition.fields")


    cookie = hiveClient.overwriteUnionSum(
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      table,
      persistenceDwr,
      aggExprsAlias,
      unionAggExprsAndAlias,
      groupByExprsAlias,
      partitionFields.head,
      partitionFields.tail:_*
    )
    batchTransactionCookiesCache.add(cookie)


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
      batchTransactionCookiesCache.removeAll(needCleans)
      result = needCleans.toArray
    }
    hiveClient.clean(result:_*)
  }
}
