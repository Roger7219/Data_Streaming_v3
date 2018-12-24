package com.mobikok.ssp.data.streaming.handler.dwr

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.client.{ClickHouseClient, HBaseClient, HiveClient, TransactionManager}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends com.mobikok.ssp.data.streaming.handler.Handler {

  var moduleName: String = _
  var transactionManager: TransactionManager = _
  var hbaseClient: HBaseClient = _
  var hiveClient: HiveClient = _
  var hiveContext: HiveContext = _
  var clickHouseClient: ClickHouseClient = _
  var handlerConfig: Config = _
  var globalConfig: Config = _

//  var isAsynchronous = false

  def init(moduleName: String,
           transactionManager:TransactionManager,
           hbaseClient: HBaseClient,
           hiveClient: HiveClient,
           clickHouseClient: ClickHouseClient,
           handlerConfig: Config,
           globalConfig: Config, // 获取group by，agg字段
           expr: String,
           as: String): Unit = {
    this.moduleName = moduleName
    this.transactionManager = transactionManager
    this.hbaseClient = hbaseClient
    this.hiveClient = hiveClient
    this.hiveContext = hiveClient.hiveContext
    this.clickHouseClient = clickHouseClient
    this.handlerConfig = handlerConfig
    this.globalConfig = globalConfig

    try {
      isAsynchronous = handlerConfig.getBoolean("asynchronous")
    } catch {
      case e: Exception =>
    }
  }

  def handle(persistenceDwr: DataFrame): DataFrame // 返回新的groupby字段集

  def prepare(dwi: DataFrame): DataFrame = dwi

}
