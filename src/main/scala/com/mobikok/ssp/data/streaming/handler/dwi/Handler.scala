package com.mobikok.ssp.data.streaming.handler.dwi

import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext;

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends Transactional{

  var LOG: Logger = null
  var moduleName: String = null
  var hbaseClient:HBaseClient = null
  var hiveClient: HiveClient = null
  var kafkaClient: KafkaClient = null
  var hiveContext: HiveContext = null
  var handlerConfig: Config = null
  var rDBConfig: RDBConfig = null

  var transactionManager: TransactionManager = null
  var exprStr: String = null
  var as: Array[String] = null

  def init (moduleName: String,
            transactionManager:TransactionManager,
            rDBConfig: RDBConfig,
            hbaseClient: HBaseClient,
            hiveClient: HiveClient,
            kafkaClient: KafkaClient,
            handlerConfig: Config,
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
    this.exprStr = expr
    this.as = as
  }

  def handle (newDwi: DataFrame): (DataFrame, Array[TransactionCookie])

  def sql(sqlText: String): DataFrame ={
    LOG.warn("Execute HQL", sqlText)
    hiveClient.hiveContext.sql(sqlText)
  }
}
