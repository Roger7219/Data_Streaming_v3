package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionRoolbackedCleanable}
import com.mobikok.ssp.data.streaming.util.{MC, ModuleTracer, OM, PushReq}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HBaseDWIPersistHandler extends Handler {

  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable: String = _


  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, moduleTracer: ModuleTracer): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, moduleTracer)

    if(globalConfig.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")){
      dwiPhoenixSubtableEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
    }
    dwiPhoenixTable = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.table")
  }

  override def doHandle(newDwi: DataFrame): DataFrame = {
    if (newDwi.count() > 0) {
      val dwiPhoenixHBaseStorableClass = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
      val cls = Class.forName(dwiPhoenixHBaseStorableClass).asInstanceOf[Class[_ <: HBaseStorable]]
      val dwiP = newDwi.toJSON.rdd.map{ x => OM.toBean(x, cls)}
//      LOG.warn()

      if (dwiPhoenixSubtableEnable) {
        hbaseClient.asInstanceOf[HBaseClient].putsNonTransactionMultiSubTable(dwiPhoenixTable, dwiP)
      } else {
        hbaseClient.putsNonTransaction(dwiPhoenixTable, dwiP)
      }
      // 收集当前批次数据总量
      MC.push(PushReq(HBASE_WRITECOUNT_BATCH_TOPIC, newDwi.count().toString))

      LOG.warn("hbaseClient putsNonTransaction done")
    }
    newDwi
  }

  override def doCommit(): Unit = {
    //空方法，因为handle()中调用的是非事务方法putsNonTransaction()，返回的cookie为空，即非真正的事务操作
  }

  override def doClean(): Unit = {
    //空方法，因为handle()中调用的是非事务方法putsNonTransaction()，返回的cookie为空，即非真正的事务操作
  }
}
