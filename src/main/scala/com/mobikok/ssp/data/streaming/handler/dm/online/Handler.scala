package com.mobikok.ssp.data.streaming.handler.dm.online

import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.{Logger, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends com.mobikok.ssp.data.streaming.handler.Handler {

  var LOG: Logger = _
  var moduleName: String = _
  var transactionManager: TransactionManager = _
  var hbaseClient: HBaseClient = _
  var hiveContext: HiveContext = _
  var handlerConfig: Config = _
  var globalConfig: Config = _
  var messageClient: MessageClient = _
  var kylinClientV2: KylinClientV2 = _
  var rDBConfig: RDBConfig = _
  var greenplumClient: GreenplumClient = _
  //  var bigQueryClient: BigQueryClient = _
  var clickHouseClient: ClickHouseClient = _

  var kafkaClient: KafkaClient = _

//  var isAsynchronous = true

  def init(moduleName: String, transactionManager: TransactionManager, clickHouseClient: ClickHouseClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config, globalConfig: Config): Unit = {
    LOG = new Logger(moduleName, getClass.getName, new Date().getTime)

    this.moduleName = moduleName
    this.transactionManager = transactionManager
    this.rDBConfig = rDBConfig
    this.hbaseClient = hbaseClient
    this.hiveContext = hiveContext
    this.handlerConfig = handlerConfig
    this.globalConfig = globalConfig
    this.messageClient = messageClient
    this.kylinClientV2 = kylinClientV2
    this.kafkaClient = kafkaClient
//    this.greenplumClient = greenplumClient
//    this.bigQueryClient = bigQueryClient
    this.clickHouseClient = clickHouseClient
  }

  def handle(dataFrame: DataFrame): Unit = {
    val className = getClass.getSimpleName.replace("$", "")
    var templateSql: String = null
    try {
      // handler配置下的sql
      templateSql = handlerConfig.getString(s"sql").replace("{className}", className)
      dataFrame.createOrReplaceTempView(s"${className}_dwr")
    }
    doHandle(if (templateSql == null) dataFrame else sql(templateSql))
  }

  protected def doHandle(dataFrame: DataFrame)

  def sql(sqlText: String): DataFrame = {
    var res: DataFrame = null
    RunAgainIfError.run({
      LOG.warn("Executing HQL", sqlText)
      res = hiveContext.sql(sqlText)
    })
    res
  }
}
