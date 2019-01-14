package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.util.{MC, OM, PushReq, ThreadPool}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class HiveDWIPersistHandler extends Handler {

//  val COOKIE_KIND_DWI_T = "dwiT"

  // 有配置表示enable
  var table: String = _
  var cookie: TransactionCookie = _

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, globalConfig, expr, as)
    table = globalConfig.getString(s"modules.$moduleName.dwi.table")
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies: _*)
  }


  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {
    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwi.partition.fields")

    val ps = newDwi
      .dropDuplicates(partitionFields)
      .collect()
      .map { x =>
        partitionFields.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }.toArray
      }
    cookie = hiveClient.into(
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      table,
      newDwi,
      ps
    )

    LOG.warn("hiveClient.into dwiTable completed", cookie)
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
    hiveClient.clean(cookies: _*)
  }

}
