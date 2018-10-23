package com.mobikok.ssp.data.streaming.handler.dwr

import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

class AfterFilterHandler extends Handler {

  var LOG: Logger = _

  var hiveContext: HiveContext = _
  var handlerConfig: Config = _
  var isMaster = false
  var hbaseClient: HBaseClient = _

  var dwrWhere: String = _

  override def init(moduleName: String, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config, expr: String, as: String): Unit = {
    LOG = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

    this.hbaseClient = hbaseClient
    this.hiveContext = hiveContext
    this.handlerConfig = handlerConfig

    // 没有则抛出异常
    this.dwrWhere = handlerConfig.getString("where")
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {
    persistenceDwr.where(dwrWhere)
  }

  override def prepare(dwi: DataFrame): DataFrame = {
    dwi
  }
}
