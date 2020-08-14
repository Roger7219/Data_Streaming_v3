package com.mobikok.ssp.data.streaming.handler.dm

import java.util.Date

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{Logger, MessageClient, ModuleTracer, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends com.mobikok.ssp.data.streaming.handler.Handler {

  var LOG: Logger = _
  var hbaseClient: HBaseClient = _
  var hiveContext: HiveContext = _
  var argsConfig: ArgsConfig = _
  var handlerConfig: Config = _
  var messageClient: MessageClient = _
  var rDBConfig: RDBConfig = _
  var bigQueryClient: BigQueryClient = _
  var clickHouseClient: ClickHouseClient = _
  var moduleTracer: ModuleTracer = _

  var kafkaClient: KafkaClient = _

  def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    LOG = new Logger(moduleName, getClass, new Date().getTime)

    this.rDBConfig = rDBConfig
    this.hbaseClient = hbaseClient
    this.hiveContext = hiveContext
    this.argsConfig = argsConfig
    this.handlerConfig = handlerConfig
    this.messageClient = messageClient
    this.kafkaClient = kafkaClient
    this.bigQueryClient = bigQueryClient
    this.clickHouseClient = clickHouseClient
    this.moduleTracer = moduleTracer
  }

  protected def doHandle(): Unit

  final def handle(): Unit ={
    moduleTracer.trace(s"dm ${getClass.getSimpleName} handle start")
    doHandle()
    moduleTracer.trace(s"dm ${getClass.getSimpleName} handler done")
  }


  def sql(sqlText: String): DataFrame = {
    var res: DataFrame = null
    RunAgainIfError.run({
      res = sql0(sqlText)
    })
    res
  }

  def sql0(sqlText: String): DataFrame ={
    LOG.warn("Execute HQL", sqlText)

    // Fix bug: https://issues.apache.org/jira/browse/SPARK-22686
    var level: Level = null;
    var log: org.apache.log4j.Logger = null
    if(sqlText.contains("if exists")) {
      level = org.apache.log4j.Logger.getLogger(classOf[DropTableCommand]).getLevel
      log = org.apache.log4j.Logger.getLogger(classOf[DropTableCommand])
      log.setLevel(Level.ERROR)
    }

    var result = hiveContext.sql(sqlText)

    // Fix bug: https://issues.apache.org/jira/browse/SPARK-22686
    if(sqlText.contains("if exists")) {
      log.setLevel(level)
    }
    result
  }
}
