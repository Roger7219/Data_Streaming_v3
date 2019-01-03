package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.util.{MC, OM, PushReq}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HBaseDWIPersistHandler extends Handler {

  var cookie: TransactionCookie = _

  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable: String = _


  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, globalConfig, expr, as)

    dwiPhoenixSubtableEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
    dwiPhoenixTable = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.table")
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hbaseClient.rollback(cookies:_*)
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {
    if (newDwi.count() > 0) {
      val dwiPhoenixHBaseStorableClass = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
      val cls = Class.forName(dwiPhoenixHBaseStorableClass).asInstanceOf[Class[_ <: HBaseStorable]]
      val dwiP = newDwi.toJSON.rdd.map{ x => OM.toBean(x, cls)}
//      LOG.warn()

      if (dwiPhoenixSubtableEnable) {
        hbaseClient.asInstanceOf[HBaseMultiSubTableClient].putsNonTransactionMultiSubTable(dwiPhoenixTable, dwiP)
      } else {
        hbaseClient.putsNonTransaction(dwiPhoenixTable, dwiP)
      }
      // 收集当前批次数据总量
      MC.push(PushReq(HBASE_WRITECOUNT_BATCH_TOPIC, newDwi.count().toString))

      LOG.warn("hbaseClient putsNonTransaction done")
    }
    (newDwi, Array())
  }

  override def commit(c: TransactionCookie): Unit = {
    hbaseClient.commit(cookie)
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    hbaseClient.clean(cookies:_*)
  }
}
