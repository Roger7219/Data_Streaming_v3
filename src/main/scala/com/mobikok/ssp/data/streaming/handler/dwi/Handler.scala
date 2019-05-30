package com.mobikok.ssp.data.streaming.handler.dwi

import java.util.Date

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends Transactional with com.mobikok.ssp.data.streaming.handler.Handler {

  var LOG: Logger = _
  var moduleName: String = _
  var hbaseClient:HBaseClient = _
  var hiveClient: HiveClient = _
  var kafkaClient: KafkaClient = _
  var hiveContext: HiveContext = _
  var handlerConfig: Config = _
  var globalConfig: Config = _
  var rDBConfig: RDBConfig = _

  var transactionManager: TransactionManager = _
  var exprStr: String = _
  var as: Array[String] = _

//  var isAsynchronous = false // 表示该handler是否异步执行

  override def init(): Unit = {}

  def init (moduleName: String,
            transactionManager:TransactionManager,
            rDBConfig: RDBConfig,
            hbaseClient: HBaseClient,
            hiveClient: HiveClient,
            kafkaClient: KafkaClient,
            handlerConfig: Config,
            globalConfig: Config,
            expr: String,
            as: Array[String]): Unit = {

    LOG = new Logger(moduleName, getClass.getName, new Date().getTime)

    this.moduleName = moduleName
    this.transactionManager = transactionManager
    this.rDBConfig = rDBConfig
    this.hbaseClient = hbaseClient
    this.hiveClient = hiveClient
    this.kafkaClient = kafkaClient
    this.hiveContext = hiveClient.hiveContext
    this.handlerConfig = handlerConfig
    this.globalConfig = globalConfig
    this.exprStr = expr
    this.as = as

    try {
      isAsynchronous = handlerConfig.getBoolean("isAsynchronous")
    } catch {
      case e: Exception =>
    }
  }

  def handle (newDwi: DataFrame): (DataFrame, Array[TransactionCookie])

  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
    hiveClient.sql(sqlText)
  }
}
