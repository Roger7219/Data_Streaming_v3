package com.mobikok.ssp.data.streaming.handler.dwr

import java.util.Date

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionalHandler}
import com.mobikok.ssp.data.streaming.util.{Logger, MessageClient, ModuleTracer, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler extends TransactionalHandler with com.mobikok.ssp.data.streaming.handler.Handler {

  var LOG: Logger = _
  var handlerName: String = _
  var moduleName: String = _
  var transactionManager: TransactionManager = _
  var hbaseClient: HBaseClient = _
  var hiveClient: HiveClient = _
  var hiveContext: HiveContext = _
  var clickHouseClient: ClickHouseClient = _
  var handlerConfig: Config = _
  var globalConfig: Config = _
  var moduleConfig: Config = _
  var messageClient: MessageClient = _
  var moduleTracer: ModuleTracer = _

  def init(moduleName: String,
           handlerName: String,
           transactionManager: TransactionManager,
           hbaseClient: HBaseClient,
           hiveClient: HiveClient,
           clickHouseClient: ClickHouseClient,
           handlerConfig: Config,
           globalConfig: Config, // 获取group by，agg字段
           messageClient: MessageClient,
           moduleTracer: ModuleTracer): Unit = {
    LOG = new Logger(moduleName, getClass, new Date().getTime)
    this.moduleName = moduleName
    this.handlerName = handlerName
    this.transactionManager = transactionManager
    this.hbaseClient = hbaseClient
    this.hiveClient = hiveClient
    this.hiveContext = hiveClient.hiveContext
    this.clickHouseClient = clickHouseClient
    this.handlerConfig = handlerConfig
    this.globalConfig = globalConfig
    this.moduleConfig = globalConfig.getConfig(s"modules.$moduleName")
    this.messageClient = messageClient
    this.moduleTracer = moduleTracer
    try {
      isAsynchronous = handlerConfig.getBoolean("async")
    } catch {case _: Exception =>}
  }

  // 子类实现
  protected def doPrepare(dwi: DataFrame): DataFrame = dwi
  protected def doHandle(dwr: DataFrame): DataFrame
  protected def doCommit(): Unit
  protected def doClean(): Unit

  /**
    * 同步和异步的handler都要执行预处理
    */
  final def prepare(dwi: DataFrame): DataFrame = {
    var returnDwrDwi = dwi
    LOG.warn(s"dwr ${title} prepare start")
    moduleTracer.trace(s"dwr ${title} prepare start")

    returnDwrDwi = doPrepare(dwi)

    LOG.warn(s"dwr ${title} prepare done")
    moduleTracer.trace(s"dwr ${title} prepare DONE")
    returnDwrDwi
  }

  final def handle(dwr: DataFrame): DataFrame ={
    var returnDwr = dwr
    LOG.warn(s"dwr ${title} handle start")
    moduleTracer.trace(s"dwr ${title} handle start")

    returnDwr = doHandle(dwr)

    LOG.warn(s"dwr ${title} handle done")
    moduleTracer.trace(s"dwr ${title} handle DONE")
    returnDwr
  }

  final def commit(): Unit={
    LOG.warn(s"dwr ${title} commit start")
    moduleTracer.trace(s"dwr ${title} commit start")

    doCommit()

    LOG.warn(s"dwr ${title} commit done")
    moduleTracer.trace(s"dwr ${title} commit DONE")
  }
  final def clean(): Unit={
    LOG.warn(s"dwr ${title} clean start")
    moduleTracer.trace(s"dwr ${title} clean start")

    doClean()

    LOG.warn(s"dwr ${title} clean done")
    moduleTracer.trace(s"dwr ${title} clean DONE")
  }

  private def title: String = {
    if(StringUtil.isEmpty(handlerName)) getClass.getSimpleName else s"${getClass.getSimpleName} $handlerName"
  }


  def sql(sqlText: String): DataFrame ={
    //    LOG.warn("Execute HQL", sqlText)
    hiveClient.sql(sqlText)
  }
}
