package com.mobikok.ssp.data.streaming.handler.dm.online

import java.util

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ClickHouseDMPersistHandler extends Handler with Transactional {

//  val COOKIE_KIND_DWR_CLICKHOUSE_T = "dwrClickHouseT"

  var hiveTable: String = _
  var cookie: TransactionCookie = _
  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()
//  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  override def init(): Unit = {}

  override def init(moduleName: String, transactionManager: TransactionManager, clickHouseClient: ClickHouseClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config, globalConfig: Config): Unit = {
    super.init(moduleName, transactionManager, clickHouseClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig, globalConfig)

    hiveTable = handlerConfig.getString("hive.table")
    isAsynchronous = true
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    clickHouseClient.rollback(cookies:_*)
  }

  // handle 替代之前的overwriteUnionSum
  override def doHandle(persistenceDwr: DataFrame): Unit = {

    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwr.partition.fields") // l_time, b_date, b_time

    cookie = clickHouseClient.overwriteUnionSum (
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      hiveTable,
      handlerConfig.getString(s"clickhouse.table"),
      persistenceDwr,
      partitionFields.head,
      partitionFields.tail:_*
    )

    batchTransactionCookiesCache.add(cookie)
  }

  override def commit(c: TransactionCookie): Unit = {
    clickHouseClient.commit(cookie)
  }


  override def clean(cookies: TransactionCookie*): Unit = {
    var result = Array[TransactionCookie]()

    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
    if (mixTransactionManager.needTransactionalAction()) {
      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
      batchTransactionCookiesCache.removeAll(needCleans)
      result = needCleans.toArray
    }
    clickHouseClient.clean(result:_*)
  }

}
