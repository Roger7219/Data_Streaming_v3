package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.HiveTransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.transaction.TransactionManager
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HiveDWIPersistHandler extends Handler {

  // 有配置表示enable
  var table: String = _
  var cookie: HiveTransactionCookie = _

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)
    isAsynchronous = true
    table = globalConfig.getString(s"modules.$moduleName.dwi.table")
  }

  override def doHandle(newDwi: DataFrame): DataFrame = {
    val partitionFields = Array("repeated", "l_time", "b_date", "b_time"/*, "b_version"*/)

    if(isOverwriteFixedLTime) {
      cookie = hiveClient.overwrite(
        transactionManager.getCurrentTransactionParentId(),
        table,
        isOverwriteFixedLTime,
        newDwi,
        partitionFields.head,
        partitionFields.tail:_*)
    } else {
      cookie = hiveClient.into(
        transactionManager.getCurrentTransactionParentId(),
        table,
        newDwi,
        partitionFields.head,
        partitionFields.tail:_*)
    }

    transactionManager.collectTransactionCookie(hiveClient, cookie)

    LOG.warn("hiveClient write dwiTable completed", cookie)
    newDwi
  }

  override def doCommit(): Unit = {
    hiveClient.commit(cookie)

    // push message
    val dwiT = cookie
    var topic = dwiT.targetTable
    if (dwiT != null && dwiT.partitions != null && dwiT.partitions.nonEmpty) {
      val key = OM.toJOSN(dwiT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      messageClient.push(PushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

      topic = moduleName
      messageClient.push(PushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

    } else {
      LOG.warn(s"MessageClient dwi no hive partitions to push", s"topic: $topic")
    }
    cookie = null
  }

  override def doClean(): Unit = {
    transactionManager.cleanLastTransactionCookie(hiveClient)
  }

}










//    batchTransactionCookiesCache.add(cookie)
//  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()
//    var result = Array[TransactionCookie]()
//
//    val mixTransactionManager = transactionManager
//    if (mixTransactionManager.needRealTransactionalAction()) {
//      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
//      batchTransactionCookiesCache.removeAll(needCleans)
//      result = needCleans.toArray
//    }
//    hiveClient.clean(result:_*)