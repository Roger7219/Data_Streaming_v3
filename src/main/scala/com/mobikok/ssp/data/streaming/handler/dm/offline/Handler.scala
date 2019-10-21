package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.Logger
import com.mobikok.ssp.data.streaming.util.RunAgainIfError
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
  var kylinClientV2: KylinClientV2 = _
  var rDBConfig: RDBConfig = _
  var greenplumClient: GreenplumClient = _
  var bigQueryClient: BigQueryClient = _
  var clickHouseClient: ClickHouseClient = _

  var kafkaClient: KafkaClient = _

  def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    LOG = new Logger(moduleName, getClass.getName, new Date().getTime)

    this.rDBConfig = rDBConfig
    this.hbaseClient = hbaseClient
    this.hiveContext = hiveContext
    this.argsConfig = argsConfig
    this.handlerConfig = handlerConfig
    this.messageClient = messageClient
    this.kylinClientV2 = kylinClientV2
    this.kafkaClient = kafkaClient
    this.greenplumClient = greenplumClient
    this.bigQueryClient = bigQueryClient
  }

  def handle()

  def setClickHouseClient(clickHouseClient: ClickHouseClient): Unit = {
    this.clickHouseClient = clickHouseClient
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
