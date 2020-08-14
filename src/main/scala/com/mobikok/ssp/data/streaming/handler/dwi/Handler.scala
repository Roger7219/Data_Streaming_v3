package com.mobikok.ssp.data.streaming.handler.dwi

import java.util.Date

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionalHandler}
import com.mobikok.ssp.data.streaming.util.{Logger, MessageClient, ModuleTracer}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends TransactionalHandler with com.mobikok.ssp.data.streaming.handler.Handler {

  var LOG: Logger = _
  var moduleName: String = _
  var transactionManager: TransactionManager = _
  var hbaseClient:HBaseClient = _
  var hiveClient: HiveClient = _
  var kafkaClient: KafkaClient = _
  var hiveContext: HiveContext = _
  var argsConfig: ArgsConfig = _
  var handlerConfig: Config = _
  var globalConfig: Config = _
  var rDBConfig: RDBConfig = _
  var version: String = _
  var moduleConfig: Config = _
  var isOverwriteFixedLTime: Boolean = _
  var moduleTracer: ModuleTracer = _

  var messageClient: MessageClient = _

  def init (moduleName: String,
            transactionManager:TransactionManager,
            rDBConfig: RDBConfig,
            hbaseClient: HBaseClient,
            hiveClient: HiveClient,
            kafkaClient: KafkaClient,
            argsConfig: ArgsConfig,
            handlerConfig: Config,
            globalConfig: Config,
            messageClient: MessageClient,
            moduleTracer: ModuleTracer): Unit = {

    LOG = new Logger(moduleName, getClass, new Date().getTime)

    this.moduleName = moduleName
    this.transactionManager = transactionManager
    this.rDBConfig = rDBConfig
    this.hbaseClient = hbaseClient
    this.hiveClient = hiveClient
    this.kafkaClient = kafkaClient
    this.hiveContext = hiveClient.hiveContext
    this.argsConfig = argsConfig
    this.handlerConfig = handlerConfig
    this.globalConfig = globalConfig
    this.version = argsConfig.get(ArgsConfig.VERSION, ArgsConfig.Value.VERSION_DEFAULT)
    this.moduleConfig = globalConfig.getConfig(s"modules.$moduleName")
    this.isOverwriteFixedLTime = if(moduleConfig.hasPath("overwrite")) moduleConfig.getBoolean("overwrite") else false
    this.messageClient = messageClient
    this.moduleTracer = moduleTracer

    try {
      isAsynchronous = handlerConfig.getBoolean("async")
    } catch {
      case _: Exception =>
    }
  }

  protected def doHandle (newDwi: DataFrame): DataFrame
  protected def doCommit(): Unit
  protected def doClean(): Unit

  final def handle (newDwi: DataFrame): DataFrame = {
    var returnDwi: DataFrame = newDwi
    LOG.warn(s"dwi ${getClass.getSimpleName} handle start")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} handle start")

    returnDwi = doHandle(newDwi)

    LOG.warn(s"dwi ${getClass.getSimpleName} handle done")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} handle done")
    returnDwi
  }

  final def commit(): Unit={
    LOG.warn(s"dwi ${getClass.getSimpleName} commit start")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} commit start")

    doCommit()

    LOG.warn(s"dwi ${getClass.getSimpleName} commit done")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} commit done")
  }
  final def clean(): Unit={
    LOG.warn(s"dwi ${getClass.getSimpleName} clean start")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} clean start")

    doClean()

    LOG.warn(s"dwi ${getClass.getSimpleName} clean done")
    moduleTracer.trace(s"dwi ${getClass.getSimpleName} clean done")
  }

  def sql(sqlText: String): DataFrame ={
    hiveClient.sql(sqlText)
  }
}
