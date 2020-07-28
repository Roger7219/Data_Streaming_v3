package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.util.{MC, OM, PushReq}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HBaseDWIPersistHandler extends Handler {

  // 暂时没有用，因为调用的是putsNonTransaction()，即非事务操作。
  var cookie: TransactionCookie = _

  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable: String = _


  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)

    if(globalConfig.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")){
      dwiPhoenixSubtableEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
    }
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
    //这样代码可以删（目前作为样例），即不用调，因为调用的是putsNonTransaction()，返回的cookie为空，即非事务操作
    hbaseClient.commit(cookie)
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    //暂时没有用，因为调用的是putsNonTransaction()，即非事务操作。
    hbaseClient.clean(Array[TransactionCookie]():_*)// 空数组
  }
}
