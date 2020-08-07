package com.mobikok.ssp.data.streaming.module

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.Date

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.protobuf.Message
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveRollbackableTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, OffsetRange, UuidStat}
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.dm.Handler
import com.mobikok.ssp.data.streaming.module.support.AlwaysTransactionalStrategy
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Administrator on 2017/6/8.
  */
@deprecated
class GenericModule (config: Config,
                     argsConfig: ArgsConfig,
                     concurrentGroup: String,
                     moduleName: String,
                    /* dwiStructType: StructType,*/
                     ssc: StreamingContext) extends Module {

  //  private[this] val LOGGER = Logger.getLogger(getClass().getName());
  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  val appName = ssc.sparkContext.getConf.get("spark.app.name")
  val ksc = Class.forName(config.getString(s"modules.${moduleName}.dwi.kafka.schema"))
  val dwiStructType = ksc.getMethod("structType").invoke(ksc.newInstance()).asInstanceOf[StructType]

  val dwiUuidFieldsSeparator = "^"

  //Value Type: String or Array[Byte]
  var stream:InputDStream[ConsumerRecord[String, Object]] = null

  val hiveContext = new HiveContext(ssc.sparkContext)
  val sqlContext = new SQLContext(ssc.sparkContext)
  var kylinClient: KylinClientV2 = null
  try {
    kylinClient = new KylinClientV2(config.getString("kylin.client.url"))
  }catch {
    case e:Exception=> LOG.warn("KylinClient initPolling fail !!", e.getMessage)
  }

  //val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
  //val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

//  var mySqlJDBCClient = new MySqlJDBCClient(
//    config.getString(s"rdb.url"),
//    config.getString(s"rdb.user"),
//    config.getString(s"rdb.password")
//  )
  var mySqlJDBCClient = new MySqlJDBCClient(
        config.getString(s"rdb.url"),
        config.getString(s"rdb.user"),
        config.getString(s"rdb.password")
      )

  var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    moduleName, config.getString(s"rdb.url"), config.getString(s"rdb.user"), config.getString(s"rdb.password")
  )

//  val greenplumJDBCClient = new GreenplumJDBCClient(
//    moduleName,
//    "jdbc:pivotal:greenplum://node15:5432;DatabaseName=postgres",
//    "gpadmin",
//    "gpadmin"
//  )
  var moduleTracer: ModuleTracer = new ModuleTracer()
  var greenplumClient = null.asInstanceOf[GreenplumClient]
  try {
    greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, transactionManager, moduleTracer)
  }catch {case e:NoClassDefFoundError => LOG.warn("GreenplumClient initPolling fail, Class no found", e.getMessage)}

  var bigQueryClient = null.asInstanceOf[BigQueryClient]
  try{
    bigQueryClient = new BigQueryClient(moduleName, config, ssc, messageClient, hiveContext, moduleTracer)
  }catch {case e:Throwable => }

  var clickHouseClient = null.asInstanceOf[ClickHouseClient]
  try {
    clickHouseClient = new ClickHouseClient(moduleName, config, ssc, messageClient, transactionManager, hiveContext, moduleTracer)
  } catch {case e: Exception => LOG.warn(s"Init clickhouse client failed, skip it, ${e.getMessage}")}

  val rDBConfig = new RDBConfig(mySqlJDBCClientV2)

  val transactionManager: MixTransactionManager = new MixTransactionManager(
    config,
    new AlwaysTransactionalStrategy(CSTTime.formatter("yyyy-MM-dd HH:00:00"), CSTTime.formatter("yyyy-MM-dd 00:00:00"))
  )//TransactionManager(config)

  val messageClient = new MessageClient(moduleName, config.getString("message.client.url"))
  val hbaseClient = new HBaseClient(moduleName, ssc.sparkContext, config, transactionManager, moduleTracer)
  val hiveClient = new HiveClient(moduleName, config, ssc, messageClient, transactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, config, transactionManager)
  val phoenixClient = new PhoenixClient(moduleName, ssc.sparkContext, config, transactionManager, moduleTracer)


  val tidTimeFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")

  var dmLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = new SimpleDateFormat(config.getString(s"modules.$moduleName.dm.load.time.format.by"))
  }catch { case _: Exception =>}

  //val dwrFirstLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var dwiLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
  try {
    dwiLoadTimeFormat = new SimpleDateFormat(config.getString(s"modules.$moduleName.dwi.load.time.format.by"))
  }catch { case _: Exception =>}
  if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
    dwiLoadTimeFormat = new SimpleDateFormat(dwiLoadTimeFormat.toPattern.substring(0, 17) + "01")
  }

  var dwrLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  try {
    dwrLoadTimeFormat = new SimpleDateFormat(config.getString(s"modules.$moduleName.dwr.load.time.format.by"))
  }catch {case _:Exception => }
  if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
    dwrLoadTimeFormat = new SimpleDateFormat(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")
  }

  var isExcludeOfflineRebrushPart = false
  if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
    isExcludeOfflineRebrushPart = true
  }

  var kafkaProtoEnable = false
  var kafkaProtoClass: Class[_] = null
  try {
    kafkaProtoEnable = config.getBoolean(s"modules.$moduleName.dwi.kafka.proto.enable")
    kafkaProtoClass = Class.forName(config.getString(s"modules.$moduleName.dwi.kafka.proto.class"))
  } catch {
    case _: Exception =>
  }

  val commitBatchSize = config.getInt(s"modules.$moduleName.commit.batch.size")

  //val dwrRollBatchSize = config.getInt(s"modules.$moduleName.dwr.roll.batch.size")
  var lastCommitTime = new Date();
  val commitTimeInterval = config.getInt(s"modules.$moduleName.commit.time.interval")

  val shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  //val dwrGroupBy = config.getStringList(s"modules.$moduleName.dwr.groupby.fields").toArray[String](new Array[String](0))

  var dwrGroupByExprs:List[Column] = null
  try {
    dwrGroupByExprs = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  }catch {case e:Exception=>}

  var dwrGroupByExprsAlias:Array[String] = null
   try{
     dwrGroupByExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
       x => x.getString("as")
     }.toArray[String]
   } catch {case e:Exception=>}

  var dwrGroupByExprsAliasCol:List[Column] =null
  try{
    dwrGroupByExprsAliasCol = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => col(x.getString("as"))
    }.toList
  }catch {case e:Exception=>}

  var aggExprs:List[Column] = null
  try {
    aggExprs = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  }catch {case e:Exception=>}

  var aggExprsAlias:List[String] = null
  try {
    aggExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
  }catch {case e:Exception=>}

  var unionAggExprsAndAlias:List[Column] = null
  try {
    unionAggExprsAndAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList
  } catch {case e:Exception=>}

  var dwiExtendedFields:List[(java.util.List[String], Column, com.mobikok.ssp.data.streaming.handler.dwi.Handler)]  = null
  try {
    dwiExtendedFields = config.getConfigList(s"modules.$moduleName.dwi.extended.fields").map {x =>
      val hc = x.getConfig("handler")
      var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
      var col = "null"
      try {
        if(StringUtil.notEmpty(x.getString("expr"))){
          col = x.getString("expr")
        }
      } catch {case ex:Exception=>}
      h.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, hc, config, col/*x.getString("expr")*/, x.getStringList("as").toArray( Array[String]() ))
        (
          x.getStringList("as"),
          expr(col),
          h
        )
    }.toList
  }catch {case e:Exception=>}

  val businessDateExtractBy = config.getString(s"modules.$moduleName.business.date.extract.by")
  val topics = config.getConfigList(s"modules.$moduleName.kafka.consumer.partitions").map { x => x.getString("topic") }.toArray[String]

  var dwiPhoenixEnable = false
  var dwiPhoenixTable = null.asInstanceOf[String]
  var dwiPhoenixHBaseStorableClass = null.asInstanceOf[String]
  try {
    dwiPhoenixEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.enable")
  }catch {case e:Exception=>}
  if(dwiPhoenixEnable) {
    dwiPhoenixTable = config.getString(s"modules.$moduleName.dwi.phoenix.table")
    dwiPhoenixHBaseStorableClass = config.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
  }

  //==
  var dwrGroupbyExtendedFields:List[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = null
  try {
    dwrGroupbyExtendedFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.extended.fields").map{ x=>
        val hc = x.getConfig("handler")
        var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
        h.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, x.getString("expr"), x.getString("as"))
        h
    }.toList
  }catch {case e:Exception =>}
  var dwrGroupbyExtendedFieldsAlias = Array[String]()
  try {
    dwrGroupbyExtendedFieldsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.extended.fields").map {
      x => x.getString("as")
    }.toArray[String]
  }catch {case  e:Exception=>}

  //==
  var dwrGroupbyExtendedAggs:List[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = null
  try {
    dwrGroupbyExtendedAggs = config.getConfigList(s"modules.$moduleName.dwr.groupby.extended.aggs").map{ x=>
      val hc = x.getConfig("handler")
      var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
      h.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, x.getString("expr"), x.getString("as"))
      h
    }.toList
  }catch {case e:Exception =>}
  var dwrGroupbyExtendedAggsAlias = Array[String]()
  try {
    dwrGroupbyExtendedAggsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.extended.aggs").map {
      x => x.getString("as")
    }.toArray[String]
  }catch {case  e:Exception=>}

  var dwrBeforeFilter: String = null
  try {
    dwrBeforeFilter = config.getString(s"modules.$moduleName.dwr.filter.before")
  }catch {case e:Exception=>}

  var dwrAfterFilter: String = null
  try {
    dwrAfterFilter = config.getString(s"modules.$moduleName.dwr.filter.after")
  }catch {case e:Exception=>}

  var dwrGreenplumEnable = false
  try {
    dwrGreenplumEnable = config.getBoolean(s"modules.$moduleName.dwr.greenplum.enable")
  }catch {case e:Exception=>}

  var dmGreenplumEenable = false
  var firstBatch = true

  try {
    dmGreenplumEenable = config.getBoolean(s"modules.$moduleName.dm.greenplum.enable")
  }catch {case e:Exception=>}

  var dmGreenplumHiveViews = null.asInstanceOf[Array[String]]
  try{
    dmGreenplumHiveViews = Array(config.getString(s"modules.$moduleName.dm.greenplum.hive.view"))
  } catch {case e:Exception=>}
  try {
    dmGreenplumHiveViews = config.getStringList(s"modules.$moduleName.dm.greenplum.hive.views").toArray(new Array[String](0))
  }catch {case e:Exception=>}

  var dmGreenplumMessageTopicsExt = Array[String]()
  try {
    dmGreenplumMessageTopicsExt = config.getStringList(s"modules.$moduleName.dm.greenplum.message.topics.ext").toArray(new Array[String](0))
  }catch {case e:Exception=>}

//  dm.greenplum.upsert.by.hive.views=[{
//    view="ssp_report_campaign_dm"
//    dwr.table="ssp_report_campaign_dwr"
//    on.conflict = "publisherId, appId, countryId, carrierId, versionName, adType, campaignId, offerId, imageId, affSub, l_time, b_date"
//  }]
  //view, dwi, dwr, conflict
  var dmGreenplumUpsertByHiveViews = null.asInstanceOf[Array[(String, String, String, Array[String])]]
  try {
    dmGreenplumUpsertByHiveViews = config.getObjectList(s"modules.$moduleName.dm.greenplum.upsert.hive.views").map{x=>
      val c = x.toConfig
      var it = null.asInstanceOf[String]
      var rt = null.asInstanceOf[String]
      var oc = null.asInstanceOf[Array[String]]
      try{ it = c.getString("dwi.table") }catch {case e: Exception=>}
      try{ rt = c.getString("dwr.table") }catch {case e: Exception=>}
      try{ oc = c.getString("on.conflict").split(",").map{x=>x.trim} }catch {case  e: Exception=>}
      (c.getString("view"), it, rt, oc)
    }.toArray
  }catch {case e:Exception=>}


  var isEnableDims = false
  var dmDimsSyncCmds: util.List[String] = null
  try {
    isEnableDims = config.getBoolean(s"modules.$moduleName.dm.dims.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableDims) {
    dmDimsSyncCmds = config.getStringList(s"modules.$moduleName.dm.dims.sync.cmds")
  }

  var isEnableKafkaDm = false
  var dmKafkaTopic: String = null
  try {
    isEnableKafkaDm = config.getBoolean(s"modules.$moduleName.dm.kafka.enable")
  } catch {
    case _: Exception =>
  }
  if(isEnableKafkaDm) {
    dmKafkaTopic = config.getString(s"modules.$moduleName.dm.kafka.topic")
  }

  var isEnablePhoenixDm = false
  var dmPhoenixTable: String = null
  var dmHBaseStorableClass: Class[_ <: HBaseStorable] = null
  try {
    isEnablePhoenixDm = config.getBoolean(s"modules.$moduleName.dm.phoenix.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnablePhoenixDm) {
    dmPhoenixTable = config.getString(s"modules.$moduleName.dm.phoenix.table")
    dmHBaseStorableClass = Class.forName(config.getString(s"modules.$moduleName.dm.phoenix.hbase.storable.class")).asInstanceOf[Class[_ <: HBaseStorable]]
  }

  var isEnableHandlerDm = false
  var dmHandlers: util.List[Handler] = null
  try {
    isEnableHandlerDm = config.getBoolean(s"modules.$moduleName.dm.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableHandlerDm) {
    dmHandlers = new util.ArrayList[Handler]()
    config.getConfigList(s"modules.$moduleName.dm.handler.setting").foreach { x =>
      var h = Class.forName(x.getString("class")).newInstance().asInstanceOf[Handler]
      h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient,kylinClient, hbaseClient, hiveContext, argsConfig, x)
      dmHandlers.add(h)
    }
  }

  //  val isEnableDm = config.getBoolean(s"modules.$moduleName.dm.enable"f)
  var isEnableDwi = false
  var dwiTable:String = null
  var dwrTable:String = null
  try{
    isEnableDwi = config.getBoolean(s"modules.$moduleName.dwi.enable")
  }catch {case e:Exception=>}
  if(isEnableDwi) {
    dwiTable = config.getString(s"modules.$moduleName.dwi.table")
  }

  var isEnableDwr = false
  try{
    isEnableDwr = config.getBoolean(s"modules.$moduleName.dwr.enable")
  }catch {case e:Exception=>}
  if(isEnableDwr) {
    dwrTable = config.getString(s"modules.$moduleName.dwr.table")
  }

  var isEnableDwiUuid = false
  var dwiUuidStatHbaseTable:String = null
  var dwiUuidFieldsAlias:String = "rowkey"
  var dwiUuidFields: Array[String] = null
  try{
    isEnableDwiUuid = config.getBoolean(s"modules.$moduleName.dwi.uuid.enable")
  }catch {case e:Exception=>}
  if(isEnableDwiUuid) {
    dwiUuidFields = //Array(Array("appId", "jarId", "imei"), Array("b_date","appId", "jarId", "imei")) //
     config.getStringList(s"modules.$moduleName.dwi.uuid.fields").toArray[String](new Array[String](0))
    try {
      dwiUuidFieldsAlias = config.getString(s"modules.$moduleName.dwi.uuid.alias")
    }catch {case e:Exception=>}

    dwiUuidStatHbaseTable = dwiTable + "_uuid"
    try{
      dwiUuidStatHbaseTable = config.getString(s"modules.$moduleName.dwi.uuid.stat.hbase.table")
    }catch {case e:Exception=>}
  }

//  table = "app", on = "app.id  = dwi.appid", select = "app.publisherId"
  var dwiLeftJoin:List[(String, String, String)] = null
  try{
    dwiLeftJoin = config.getConfigList(s"modules.$moduleName.dwi.join.left").map{x=>
      (x.getString("table"), x.getString("on"), x.getString("select"))
    }.toList
  } catch {case e:Exception=>}


  var dwrIncludeRepeated = true
  try{
    dwrIncludeRepeated = config.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  }catch {case e:Exception=>}

  @volatile var cacheGroupByDwr: DataFrame = null
  var cacheUuidRepeats: DataFrame = null
  @volatile var cacheDwi: DataFrame = null

//  var isEnableKylinDm = false
//  var cubes: util.List[String] = null
//  try {
//    isEnableKylinDm = config.getBoolean(s"modules.$moduleName.dm.kylin.enable")
//  } catch {
//    case _: Exception =>
//  }
//  if (isEnableKylinDm) {
//    cubes = config.getStringList(s"modules.$moduleName.dm.kylin.cubes")
//    kylinClient = new KylinClientV2(
//      config.getString(s"rdb.url"),
//      config.getString(s"rdb.user"),
//      config.getString(s"rdb.password")
//    )
//  }

  var isMaster = true
//  override def getName(): String = moduleName
//  override def isInitable(): Boolean = true
//  override def isMaster(): Boolean = true
//  override def isRunnable(): Boolean = true

  override def init (): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} initPolling started", moduleName)

      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      hiveClient.init()
      hbaseClient.init()
      kafkaClient.init()
      phoenixClient.init()

      hiveClient.rollback()
      hbaseClient.rollback()
      kafkaClient.rollback()
      phoenixClient.rollback()

      mySqlJDBCClient.execute(
        s"""
           |  insert into module_running_status(
           |    app_name,
           |    module_name,
           |    update_time,
           |    rebrush,
           |    batch_buration
           |  )
           |  values(
           |    "$appName",
           |    "$moduleName",
           |    now(),
           |    "${argsConfig.get(ArgsConfig.REBRUSH, "NA").toUpperCase}",
           |    ${(100.0*config.getInt("spark.conf.streaming.batch.buration")/60).asInstanceOf[Int]/100.0}
           |  )
           |  on duplicate key update
           |    app_name = values(app_name),
           |    update_time = values(update_time),
           |    rebrush = values(rebrush),
           |    batch_buration = values(batch_buration)
           """.stripMargin)

      LOG.warn(s"${getClass.getSimpleName} initPolling completed", moduleName)
    } catch {
      case e: Exception =>
        throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' initPolling fail, Transactionals rollback exception", e)
    }
  }

  def start (): Unit = {

  }

  //  def LOG(name: String, value: Any, logLastTime: Long): Long ={
  //    val t =new Date().getTime
  //
  //    LOGGER.warn(
  //      s"""\n
  //         |-------------------------------------------- $name --------------------------------------------
  //         |${JSON.toJOSN(value)}
  //         |\n
  //         |Using time: ${t - logLastTime}
  //         |\n
  //         | """.stripMargin)
  //
  //    return t
  //  }

  var dwiExtendedFieldsHandlerCookies: Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])] = Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()


  val traceBatchUsingTimeFormat = new DecimalFormat("#####0.00")

//  def traceBatchUsingTime (title: String, lastTraceTime: Array[Long], traceBatchUsingTimeLog: ListBuffer[String]) = {
//    val ms = new Date().getTime - lastTraceTime(0)
//    val m = traceBatchUsingTimeFormat.format((100.0*ms/1000/60).asInstanceOf[Int]/100.0)
//    traceBatchUsingTimeLog.append(s"$m  $title")
//    lastTraceTime.update(0, new Date().getTime)
//  }

  @volatile var moduleReadingKafkaMarks = mutable.Map[String, Boolean]()

  def handler (): Unit = {

    val lock = new Object
    lock.synchronized{
      new Thread(new Runnable {
        override def run (): Unit = {
          lock.synchronized{
            stream = kafkaClient.createDirectStream(config, ssc, kafkaClient.getCommitedOffset(topics: _*), moduleName)
            lock.notifyAll()
          }

        }
      }).start()

      lock.wait()
    }
    // kafkaClient.createDirectStream(topics(0), config, ssc, moduleName)
    stream.start()

    stream.foreachRDD { source =>
      try {

      //var batchBeginTime = new Date().getTime
//      var batchUsingTime = 0.asInstanceOf[Long]
//      var lastTraceTime = Array(new Date().getTime)
//      var traceBatchUsingTimeLog = mutable.ListBuffer[String]()
      moduleTracer.startBatch

      val offsetRanges = source
        .asInstanceOf[HasOffsetRanges]
        .offsetRanges
        .map { x =>
          OffsetRange(x.topic, x.partition, x.fromOffset, x.untilOffset)
        }

      val offsetDetail = kafkaClient
        .getLatestOffset(offsetRanges.map{ x=> x.topic})
        .map{x=>
          val v = offsetRanges.filter(o=> o.topic.equals(x._1.topic) && o.partition.equals(x._1.partition))
          // topic, partition, cnt, lag
          if(v.isEmpty) {
            (x._1, "NA", "NA")
          }else {
            (x._1, v(0).untilOffset - v(0).fromOffset, x._2 - v(0).untilOffset)
          }
        }

      moduleTracer.trace("offset detail\n" + offsetDetail.map{x=>s"        ${x._1.topic}, ${x._1.partition} -> cnt: ${x._2} lag: ${x._3}"}.mkString("\n"))
//      traceBatchUsingTime("offset detail\n" + offsetDetail.map{x=>s"        ${x._1.topic}, ${x._1.partition} -> cnt: ${x._2} lag: ${x._3}"}.mkString("\n"), lastTraceTime, traceBatchUsingTimeLog)

      moduleTracer.pauseBatch()
      while (moduleReadingKafkaMarks.getOrElse(moduleName, false)) {
        LOG.warn("Waiting for the last batch kafka data read to complete !!", offsetRanges)
        Thread.sleep(1000*10)
      }
      moduleReadingKafkaMarks.put(moduleName, true)

      moduleTracer.trace("wait last batch")
      moduleTracer.continueBatch()

      LOG.warn("Kafka Offset Range", offsetRanges)

      //For Task serializable
      val _kafkaProtoClass = kafkaProtoClass
      if (kafkaProtoEnable) {
        LOG.warn("Kafka Source(parsed protobuf data) take(2)", source.take(2).map { x =>
          ProtobufUtil.protobufToJSON(_kafkaProtoClass.asInstanceOf[Class[Message]], x.value().asInstanceOf[Array[Byte]])
        })
      } else {
        if(isEnableDwi) LOG.warn("Kafka Source take(2)", source.take(2).map { x => x.value() })
      }

      var jsonSource: RDD[String] = null
      if (kafkaProtoEnable) {
        //解决protobuf包冲突问题
        //        jsonSource = hiveContext.sparkContext.parallelize(source.collect.map { x =>
        //          ProtobufUtil.protobufToJSON(_kafkaProtoClass.asInstanceOf[Class[Message]], x.value().asInstanceOf[Array[Byte]])
        //        })
        jsonSource = source.map { x =>
          ProtobufUtil.protobufToJSON(_kafkaProtoClass.asInstanceOf[Class[Message]], x.value().asInstanceOf[Array[Byte]])
        }
      } else {
        jsonSource = source.map { x =>
          x.value().asInstanceOf[String]
        }
      }

      val filtered = jsonSource.filter { x =>
        x.startsWith("""{"""")
      }.repartition(shufflePartitions)

      if(isEnableDwi) LOG.warn("Filtered Rdd take(2)", filtered.take(2))
      moduleTracer.trace("read kafka")
      moduleReadingKafkaMarks.put(moduleName, false)

//      traceBatchUsingTime("read kafka", lastTraceTime, traceBatchUsingTimeLog)

      //-----------------------------------------------------------------------------------------------------------------
      // Checking concurrent group status
      //-----------------------------------------------------------------------------------------------------------------
      moduleTracer.pauseBatch()
//      batchUsingTime = batchUsingTime + (new Date().getTime - batchBeginTime)
      LOG.warn(s"Checking global app running status [ $concurrentGroup, $moduleName ]", GlobalAppRunningStatusV2.allStatusToString)
      GlobalAppRunningStatusV2.waitRunAndSetRunningStatus(concurrentGroup, moduleName)

      moduleTracer.trace("wait queue")
      moduleTracer.continueBatch()

//      batchBeginTime = new Date().getTime
//      lastTraceTime.update(0, new Date().getTime)

      var dwi = hiveContext.read
        .schema(dwiStructType)
        .json(filtered)

//      var dwiExtCookies:Array[TransactionCookie] = Array[TransactionCookie]()
      if(dwiExtendedFields != null) {
        dwiExtendedFields.foreach { x =>
          val y = x._3.handle(dwi)
          //切断rdd依赖
          dwi = hiveContext.createDataFrame(y._1.collectAsList(), y._1.schema)
          dwiExtendedFieldsHandlerCookies :+= (x._3, y._2)
        }
        moduleTracer.trace("dwi handler")
//        traceBatchUsingTime("dwi ext fields handle", lastTraceTime, traceBatchUsingTimeLog)
      }
      if (isEnableDwiUuid) {
        dwi = dwi.select(concat_ws(dwiUuidFieldsSeparator, dwiUuidFields.map { x => expr(x) }: _*).as(dwiUuidFieldsAlias), col("*"))

        //        dwi = dwi.select(
//          array(dwiUuidFields.map{x=>
//            concat_ws(dwiUuidFieldsSeparator, x.map { y => expr(y) }: _*).as(dwiUuidFieldsAlias)
//          }:_*),
//          col("*"))
      } else {
        dwi = dwi.select(expr(s"null as $dwiUuidFieldsAlias"), col("*")) //.withColumn(dwiUuidFieldsAlias, lit(""))
      }

      dwi = dwi.alias("dwi")

      //left join
      if(dwiLeftJoin != null) {
        dwiLeftJoin.foreach{x=>
          val table = hiveContext.read.table(x._1).alias(x._1)
          dwi = dwi.join(table, expr(x._2), "left_outer").selectExpr("dwi.*", x._3)
        }
        dwi = hiveContext.createDataFrame(dwi.collectAsList(), dwi.schema).repartition(shufflePartitions).alias("dwi")
      }

      // Statistical repeats
      // For spark serializable
      var _uuidDwiFieldsRowkey = this.dwiUuidFieldsAlias

      if (isEnableDwiUuid) {
        if (cacheUuidRepeats != null) {

          val ids = dwi
            .dropDuplicates(dwiUuidFieldsAlias)
            .alias("x")
            .join(cacheUuidRepeats, col(s"x.$dwiUuidFieldsAlias") === col("ur.uuid"), "left_outer")
            .where("ur.uuid is null")
            .select(s"x.$dwiUuidFieldsAlias")
            .rdd
            .map { x =>
              x.getAs[String](_uuidDwiFieldsRowkey)
            }
            .collect()

          LOG.warn("cacheUuidRepeats!=null, Filter cache uuid take(2)", ids.take(2))

          val saved = hbaseClient.getsAsDF(dwiUuidStatHbaseTable, ids, classOf[UuidStat]).alias("saved")
          //        LOG.info("HBase Client GetsAsDF Schema: " + saved.schema)

          LOG.warn("cacheUuidRepeats!=null, Get saved uuidRepeats take(2)", saved.take(2))

          cacheUuidRepeats = dwi
            .groupBy(s"dwi.$dwiUuidFieldsAlias")
            .agg((count(lit(1))).alias("repeats"))
            .alias("x")
            .join(cacheUuidRepeats, col(s"x.$dwiUuidFieldsAlias") === col("ur.uuid"), "full_outer")
            .join(saved, col(s"x.$dwiUuidFieldsAlias") === col("saved.uuid"), "full_outer")
            .selectExpr(
              s"nvl(x.$dwiUuidFieldsAlias, nvl(ur.uuid, saved.uuid)) as uuid",
              s"nvl(x.repeats, 0) + nvl(ur.repeats, 0) + nvl(saved.repeats, 0) as repeats"
            )
            .repartition(shufflePartitions)
          //          .alias("ur")

          cacheUuidRepeats = hiveContext.createDataFrame(cacheUuidRepeats.collectAsList(), cacheUuidRepeats.schema).repartition(shufflePartitions).alias("ur")

          LOG.warn("cacheUuidRepeats!=null, New cacheUuidRepeats take(2)", cacheUuidRepeats.take(2))

        } else {

          val ids = dwi
            .dropDuplicates(dwiUuidFieldsAlias)
            .select(dwiUuidFieldsAlias)
            .rdd
            .map { x =>
              x.getAs[String](_uuidDwiFieldsRowkey)
            }
            .collect()

          LOG.warn("cacheUuidRepeats==null, dwi uuid take(2)", ids.take(2))

          val saved = hbaseClient.getsAsDF(dwiUuidStatHbaseTable, ids, classOf[UuidStat]).alias("saved")

          LOG.warn("cacheUuidRepeats==null, Get saved uuidRepeats take(2)", saved.take(2))

          cacheUuidRepeats = dwi
            .groupBy(s"dwi.$dwiUuidFieldsAlias")
            .agg((count(lit(1))).alias("repeats"))
            .alias("x")
            .join(saved, col(s"x.$dwiUuidFieldsAlias") === col("saved.uuid"), "left_outer")
            .selectExpr(
              s"x.$dwiUuidFieldsAlias as uuid",
              s"x.repeats + nvl(saved.repeats, 0) as repeats"
            )
            .repartition(shufflePartitions)
          //          .alias("ur")

          cacheUuidRepeats = hiveContext.createDataFrame(cacheUuidRepeats.collectAsList(), cacheUuidRepeats.schema).repartition(shufflePartitions).alias("ur")

          LOG.warn("cacheUuidRepeats==null, New cacheUuidRepeats take(2)", cacheUuidRepeats.take(2))

          //cacheUuidRepeats.collect()
        }

        moduleTracer.trace("uuid repeats stat")
//        traceBatchUsingTime("uuid repeats stat", lastTraceTime, traceBatchUsingTimeLog)
      }

      //----------------------------------------------------------------------------------------------------------------
      var firstDwiLTime:String = null
      var firstDwrLTime:String = null
      val cd = CSTTime.uniqueSecondes()
      firstDwiLTime = dwiLoadTimeFormat.format(cd)
      firstDwrLTime = dwrLoadTimeFormat.format(cd)
      var dwiLTimeSourceField, dwrLTimeSourceField:String = "null"
      try{
        if(dwi.schema.fieldIndex("dwrLoadTime") >= 0) {
          dwiLTimeSourceField = "dwrLoadTime"
          dwrLTimeSourceField = "dwrLoadTime"
        }
      }catch {case  e:Exception=>}

      //整合离线统计设计：
      //-1.关闭流处理，启动流处理，启动离线统计
      //0.检测offline.rebrush.data.running是否为true
      //1.数据源头层为新流入数据的l_time分区值 + 1秒

      //date_format( nvl(null, '2017-09-12 00:00:00'), 'yyyy-MM-dd 00:00:00')
      var dwiLTimeExpr, dwrLTimeExpr = null.asInstanceOf[String]

      if("null".equals(dwiLTimeSourceField) ) {
        dwiLTimeExpr = s"'$firstDwiLTime'"
        dwrLTimeExpr = s"'$firstDwrLTime'"
      }else {
        dwiLTimeExpr = s"date_format($dwiLTimeSourceField, '${dwiLoadTimeFormat.toPattern}')"
        dwrLTimeExpr = s"date_format($dwrLTimeSourceField, '${dwrLoadTimeFormat.toPattern}')"
      }

      LOG.warn("dwiLTimeExpr, dwrLTimeExpr", s"$dwiLTimeExpr, $dwrLTimeExpr" );
      //----------------------------------------------------------------------------------------------------------------

      // Detail data with repeats
      var newDwi: DataFrame = null
      if (isEnableDwiUuid) {
        newDwi = dwi
          .join(cacheUuidRepeats, col(s"dwi.$dwiUuidFieldsAlias") === col("ur.uuid"), "left_outer")
          .selectExpr(
            s"nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1)) as repeats",
            s"dwi.*",
            s"if( (nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1))) = 0, 'N', 'Y') as repeated",
            s"$dwiLTimeExpr as l_time"
          )
      } else {
        newDwi = dwi
          .selectExpr(
            s"0 as repeats",
            s"dwi.*",
            s"'N' as repeated",
            s"$dwiLTimeExpr as l_time"
          )
      }

      //新流入的数据写到l_time+1000year的分区，不属于+1000year的分区就忽略掉
      if(isExcludeOfflineRebrushPart) {
        val w = s"second(l_time) > 0 "
        newDwi = newDwi.where(w)
        LOG.warn("newDwi filter by ", w)
      }
      //++
      //LOG.warn("newDwi take(2)", newDwi.take(2))

      if (cacheDwi == null) {
        cacheDwi = newDwi//.withColumn("l_time", expr(dwiLTimeExpr))
//        cacheDwi = hiveContext.createDataFrame(cacheDwi.collectAsList(), cacheDwi.schema).repartition(shufflePartitions)
      } else {
        // Merge previous grouped data
        cacheDwi = cacheDwi.union(newDwi/*.withColumn("l_time", expr(dwiLTimeExpr))*/).repartition(shufflePartitions)
        cacheDwi = hiveContext.createDataFrame(cacheDwi.collectAsList(), cacheDwi.schema).repartition(shufflePartitions)
      }

      if(isEnableDwi) LOG.warn("cacheDwi take(2)", cacheDwi.take(2))
      moduleTracer.trace("refresh cacheDwi")
//      traceBatchUsingTime("refresh cacheDwi", lastTraceTime, traceBatchUsingTimeLog)

      // Group by, Do not contain duplicate
      if (isEnableDwr) {
        if (!dwrIncludeRepeated) {
          newDwi = newDwi.where("repeated = 'N'")
        }

        var filteredNewDwi: DataFrame = null
        if(dwrBeforeFilter != null) {
          filteredNewDwi = newDwi.where(dwrBeforeFilter)
          LOG.warn("filteredNewDwi filter by dwr.filter.before", dwrBeforeFilter)
        }else {
          filteredNewDwi = newDwi
        }

        var dwr = filteredNewDwi
          //.where("repeated = 'N'")
          .withColumn("l_time", expr(dwrLTimeExpr))
          .withColumn("b_date", to_date(expr(businessDateExtractBy /*dwrGroupByDateExtractBy*/)).cast("string"))
          .groupBy(col("l_time") :: col("b_date") :: dwrGroupByExprs: _*)
          //.groupBy("b_date", dwrGroupByExprs: _*)
          .agg(/*count(lit(1)).alias("times") ,*/ aggExprs.head, aggExprs.tail: _*)
        //          .withColumn("fl_time", lit(null))
        dwr = hiveContext.createDataFrame(dwr.collectAsList(), dwr.schema).repartition(shufflePartitions)

        if(isEnableDwr) LOG.warn("New dwr take(2)", dwr.take(2))

        // fl_time: The first time the data is saved
        // aggs  : Statistics value

        if (cacheGroupByDwr == null) {
          cacheGroupByDwr = dwr
//          cacheGroupByDwr = hiveContext.createDataFrame(cacheGroupByDwr.collectAsList(), cacheGroupByDwr.schema).repartition(shufflePartitions)
          LOG.warn("cacheGroupByDwr(first new dwr) take(2)", cacheGroupByDwr.take(2))
        } else {
          // Merge previous grouped data
          cacheGroupByDwr = dwr
            .union(cacheGroupByDwr)
            .groupBy(col("l_time")::col("b_date") :: dwrGroupByExprsAliasCol: _* /*dwrGroupByExprsAlias.map(col(_)): _**/)
            //.groupBy("b_date", dwrGroupByExprs: _*)
            .agg(
              unionAggExprsAndAlias.head,
              unionAggExprsAndAlias.tail: _*
            //            :+
            //            max("fl_time").alias("fl_time"): _*
            )
            .repartition(shufflePartitions)

          cacheGroupByDwr = hiveContext.createDataFrame(cacheGroupByDwr.collectAsList(), cacheGroupByDwr.schema).repartition(shufflePartitions)

          LOG.warn("cacheGroupByDwr(unioned new dwr) take(2)", cacheGroupByDwr.take(2))
        }
        //      cacheGroupByDwr.cache()
        moduleTracer.trace("refresh cacheDwr")
//        traceBatchUsingTime("refresh cacheDwr", lastTraceTime, traceBatchUsingTimeLog)
      }

      //-----------------------------------------------------------------------------------------------------------------
      // Persistence
      //-----------------------------------------------------------------------------------------------------------------

      // Count
      if (isEnableDwiUuid) LOG.warn("cacheUuidRepeats.count()", cacheUuidRepeats.count())
      val dwiCount = cacheDwi.count()
      LOG.warn("cacheDwi.count()", dwiCount)
      if (isEnableDwr) LOG.warn("cacheGroupByDwr.count()", cacheGroupByDwr.count())

      val isTimeToCommit = (new Date().getTime - lastCommitTime.getTime) > 1000 * commitTimeInterval


//      """
//         create table config(
//           name varchar(100),
//           value varchar(100),
//           primary key (name)
//         ) ENGINE=InnoDB DEFAULT CHARSET=utf8
//      """.stripMargin
//
      moduleTracer.pauseBatch()
      var isPause = true
      while(isPause) {
        if("pause".equals(rDBConfig.readRDBConfig(RDBConfig.SPARK_DATA_STREAMING_STATUS))) {
          isPause = true
          Thread.sleep(10000)

        }else {
          isPause = false
        }
      }
      moduleTracer.continueBatch()
//      batchBeginTime = new Date().getTime
//      lastTraceTime.update(0, new Date().getTime)

      if (dwiCount >= commitBatchSize /*100000*/ || (isTimeToCommit && dwiCount > 0)) {

        val groupName = if(StringUtil.notEmpty(dwrTable)) dwrTable else moduleName
        val tid = transactionManager.beginTransaction(moduleName, groupName)
        //val tid = tidTimeFormat.format(new Date)

        var uuidT, dwrT, dwiT, dwiPhoenixT, kafkaT, dmT: TransactionCookie = null
        //try {

        if (isEnableDwiUuid) {
          uuidT = hbaseClient.puts(
            tid + TransactionManager.parentTransactionIdSeparator + "1",
            dwiUuidStatHbaseTable,
            cacheUuidRepeats
              .rdd
              .map { x =>
                UuidStat(
                  x.getAs[String]("uuid"),
                  x.getAs[Long]("repeats").toInt
                )//.setRowkey(Bytes.toBytes(x.getAs[String]("uuid")))
              }
              .collect()
          )

          LOG.warn("hbaseClient.puts uuid completed", uuidT)
          moduleTracer.trace("hbase puts uuid")
//          traceBatchUsingTime("hbase puts uuid", lastTraceTime, traceBatchUsingTimeLog)
        }

        //(b_date="2017-06-10" and l_time="2017-06-01 00:00:00")

        if (isEnableDwr) {
          if(dwrGroupbyExtendedFields != null) {
            dwrGroupbyExtendedFields.foreach{ x=>
              cacheGroupByDwr = x.handle(cacheGroupByDwr)
              cacheGroupByDwr = hiveContext.createDataFrame(cacheGroupByDwr.collectAsList(), cacheGroupByDwr.schema).repartition(shufflePartitions)
            }
            LOG.warn("hiveClient dwrExtendedFields all handlers completed, take(2)", cacheGroupByDwr.take(2))
            moduleTracer.trace("dwr extended fields handler")
//            traceBatchUsingTime("dwr extended fields handler", lastTraceTime, traceBatchUsingTimeLog)
          }

          if(dwrAfterFilter != null) {
            cacheGroupByDwr = cacheGroupByDwr.where(dwrAfterFilter)
            LOG.warn("hiveClient cacheGroupByDwr filter by dwr.filter.after", dwrAfterFilter)
          }

          dwrT = hiveClient.overwriteUnionSum(
            tid + TransactionManager.parentTransactionIdSeparator + "2",
            dwrTable,
            cacheGroupByDwr.selectExpr((
              dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias ++
                aggExprsAlias :+
                //s"times" :+
                s"l_time" :+
                s"b_date"
              ): _*),
            aggExprsAlias,
            unionAggExprsAndAlias,
            Set[String](),
            dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias,
            null,
            "l_time",
            "b_date"
          )
          LOG.warn("hiveClient.overwriteUnionSum dwrTable completed", dwrT)
          moduleTracer.trace("hvie dwr save")
//          traceBatchUsingTime("hvie dwr save", lastTraceTime, traceBatchUsingTimeLog)

          //          dwrT = hiveClient.overwrite(
          //            tid + TransactionManager.parentTransactionIdSeparator + "2",
          //            dwrTable,
          //            cacheGroupByDwr.selectExpr((
          //              dwrGroupByExprsAlias ++
          //                aggExprsAlias :+
          //                //s"times" :+
          //                s"nvl(fl_time, '${dwrFirstLoadTimeFormat.format(DateTimeUtil.uniqueSecondes())}') as fl_time" :+
          //                s"b_date"
          //              ): _*),
          //            "fl_time",
          //            "b_date"
          //          )
          //          LOG.warn("hiveClient.overwrite dwrTable completed", dwrT)

          //table: String, df: DataFrame, partitionField: String, partitionFields: String*
        }
        // l_time: The time of the data is saved
        // l_date: The date of the data is saved, Use for partition
        // b_date: The date of the data business time

        //(b_date="2017-06-10" and l_time="2017-06-21_11-00-00")
        // overwrite 1 hour data
        if (isEnableDwi) {
          dwiT = hiveClient.into(
            tid + TransactionManager.parentTransactionIdSeparator + "3",
            dwiTable,
            cacheDwi.selectExpr(
              s"*",
//              s"l_time",
              //s"'${dwiLoadTimeFormat.format(new Date())}' as l_time",
              s"cast(to_date($businessDateExtractBy) as string)  as b_date"
            ),
            "l_time",
            "b_date"
          )
          LOG.warn("hiveClient.into dwiTable completed", dwiT)
          moduleTracer.trace("hvie dwi save")
//          traceBatchUsingTime("hvie dwi save", lastTraceTime, traceBatchUsingTimeLog)
        }

        kafkaT = kafkaClient.setOffset(tid + TransactionManager.parentTransactionIdSeparator + "4", offsetRanges)

        LOG.warn("kafkaClient.setOffset completed", kafkaT)

        if (isEnableDims) {
          for (cmd <- dmDimsSyncCmds) {
            try {
              // zookeeper?
              Command.exeCmd(cmd)
            } catch {
              case e: Exception =>
                LOG.warn("Run sync dims table command exception: ", e)
            }
          }
        }

        //        if(isEnableHandlerDm) {
        //          dmHandlers.foreach{x=>
        //            x.handle()
        //          }
        //        }


        if (isEnablePhoenixDm) {
          dmT = phoenixClient.upsertUnionSum(
            tid + TransactionManager.parentTransactionIdSeparator + "5",
            dmPhoenixTable,
            dmHBaseStorableClass,
            cacheGroupByDwr.selectExpr(
              ((dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias :+
                "b_date") ++
                aggExprsAlias :+
                s"l_time"
                ): _*),
            unionAggExprsAndAlias,
            //"times",
            dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias :+ "b_date",
            "l_time"
          )

          LOG.warn("phoenixClient.upsertUnionSum completed", dmT)
        }

        if(dwiPhoenixEnable) {

          val c = Class.forName(dwiPhoenixHBaseStorableClass).asInstanceOf[Class[_<:HBaseStorable]]
          val dwiP = cacheDwi
            .selectExpr(
              s"*",
//              s"l_time",
              s"cast(to_date($businessDateExtractBy) as string)  as b_date"
            )
            .toJSON
            .rdd
            .map {x =>
              OM.toBean(x, c)
            }

          LOG.warn("hbaseClient dwi.phoenix putsNonTransaction starting, take(2)", dwiP.take(2))

          hbaseClient.putsNonTransaction(
              dwiPhoenixTable,
              dwiP
            )
          moduleTracer.trace("hbase dwi puts(non-tran)")
//          traceBatchUsingTime("hbase dwi puts(non-tran)", lastTraceTime, traceBatchUsingTimeLog)

          LOG.warn("hbaseClient dwi.phoenix putsNonTransaction completed")

//          dwiPhoenixT = hbaseClient.puts(
//            tid + TransactionManager.parentTransactionIdSeparator + "6",
//            dwiPhoenixTable,
//            dwiP,
//            c
//          )
//          LOG.warn("hbaseClient dwi.phoenix puts completed", dwiPhoenixT)

        }

        //------------------------------------------------------------------------------------
        //Commit
        //------------------------------------------------------------------------------------

        if (isEnableDwiUuid) {
          hbaseClient.commit(uuidT)
          LOG.warn("hbaseClient.commit(t1) completed(put cacheUuidRepeats)", uuidT)
          moduleTracer.trace("hbase uuid commit")
//          traceBatchUsingTime("hbase uuid save commit", lastTraceTime, traceBatchUsingTimeLog)
        }

        if (isEnableDwr) {
          hiveClient.commit(dwrT)
          LOG.warn("hiveClient.commit(t2) completed(overwrite dwr)", dwrT)
          moduleTracer.trace("hive dwr commit")
//          traceBatchUsingTime("hive dwr commit", lastTraceTime, traceBatchUsingTimeLog)

          if(dwrGreenplumEnable) {
            greenplumClient.overwrite(
              dwrTable,
              dwrTable,
              dwrT.asInstanceOf[HiveRollbackableTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct,
              "b_date");
            moduleTracer.trace("greenplum dwr refreshed")
//            traceBatchUsingTime("greenplum dwr refreshed", lastTraceTime, traceBatchUsingTimeLog)
          }
        }

        if (isEnableDwi) {
          hiveClient.commit(dwiT)
          LOG.warn("hiveClient.commit(t3) completed(into cacheDwi)", dwiT)
          moduleTracer.trace("hive dwi commit")
//          traceBatchUsingTime("hive dwi commit", lastTraceTime, traceBatchUsingTimeLog)
        }

        if(dwiExtendedFieldsHandlerCookies != null && dwiExtendedFieldsHandlerCookies.length > 0) {
          dwiExtendedFieldsHandlerCookies.foreach { x =>
            x._2.foreach{y=>
              x._1.commit(y)
              LOG.warn(s"${x._1.getClass.getSimpleName}.commit() completed", y)
            }
          }
          moduleTracer.trace("dwi handler commit")
//          traceBatchUsingTime("dwi ext fields handle commit", lastTraceTime, traceBatchUsingTimeLog)
        }


        kafkaClient.commit(kafkaT)
        LOG.warn("kafkaClient.commit(t4) completed(set offset)", kafkaT)
        moduleTracer.trace("kafka commit")
//        traceBatchUsingTime("kafka commit", lastTraceTime, traceBatchUsingTimeLog)


        //------------------------------------------------------------------------------------
        //Push Message
        //------------------------------------------------------------------------------------
        pushMessage(dwrT.asInstanceOf[HiveRollbackableTransactionCookie], dwiT.asInstanceOf[HiveRollbackableTransactionCookie])

        if (isEnableHandlerDm) {
          dmHandlers.foreach { x =>
            LOG.warn(s"dm handler(${x.getClass.getSimpleName}) starting")
            x.handle()
            LOG.warn(s"dm handler(${x.getClass.getSimpleName}) completed")
          }
          moduleTracer.trace("dm handler")
//          traceBatchUsingTime("dm handler", lastTraceTime, traceBatchUsingTimeLog)
        }


        if (isEnablePhoenixDm) {
          phoenixClient.commit(dmT)
          LOG.warn("phoenixClient.commit(t5) completed(upsertUnionSum cacheGroupByDwr)", dmT)
        }

        //------------------------------------------------------------------------------------
        //Greenplum
        //------------------------------------------------------------------------------------
        if(/*isEnableDwr && */dmGreenplumEenable) {

          val m = rDBConfig.readRDBConfig(s"${RDBConfig.GREENPLUM_WRITE_MODE}.$appName", RDBConfig.GREENPLUM_WRITE_MODE)
          if("overwrite".equals(m) /*|| firstBatch*/) {
            overwrideGreenplum()
            firstBatch = false
//            traceBatchUsingTime("greenplum dm overwrite", lastTraceTime, traceBatchUsingTimeLog)
          } else {
            upsertGreenplum(dwiT, dwrT)
//            traceBatchUsingTime("greenplum dm upsert", lastTraceTime, traceBatchUsingTimeLog)
          }
        }

//        if(dwiPhoenixEnable){
//          hbaseClient.commit(dwiPhoenixT)
//          LOG.warn("hbaseClient.commit(dwiPhoenixT) completed", dwiPhoenixT)
//        }

        if (isEnableKafkaDm) {
          val k = cacheGroupByDwr.selectExpr((
            dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias ++
              aggExprsAlias :+
              //s"times" :+
              s"l_time as dwrLoadTime" :+
              s"b_date as dwrBusinessDate"
            ): _*)
            .toJSON
            //.withColumn("l_time", expr(s"'${dwrLoadTimeFormat.format(DateTimeUtil.uniqueSecondes())}'"))
            .collect()

          LOG.warn(s"kafkaClient.send $dmKafkaTopic stating, count", k.length )
//          k.foreach { x =>
//              kafkaClient.send(dmKafkaTopic, x)
//            }
          kafkaClient.sendToKafka(dmKafkaTopic, k:_*)
          LOG.warn(s"kafkaClient.send $dmKafkaTopic completed, take(2)", k.take(2) )
          moduleTracer.trace("dwr to kafka")
//          traceBatchUsingTime("dwr to kafka", lastTraceTime, traceBatchUsingTimeLog)
        }

        //      } catch {
        //        case e: Exception =>
        //          LOG.error(classOf[GenericModule].getSimpleName + " persistence data fail, The completed portion will be rollback: ", e)

        //          if (t1 != null) hbaseClient.rollback(t1)
        //          if (t2 != null) hiveClient.rollback(t2)
        //          if (t3 != null) hiveClient.rollback(t3)
        //          if (t4 != null) kafkaClient.rollback(t4)
        //          return
        //      } finally {
        // Reset
        cacheDwi = null

        if (isEnableDwiUuid) cacheUuidRepeats = null

        if (isEnableDwr /*|| needRoll*/ ) {
          //          if (cacheGroupByDwr.count() > dwrRollBatchSize) {
          cacheGroupByDwr = null
          //            updateKylinCube(moduleName)

          //          }
        }

//        if (isEnableKylinDm) {
//          // updateKylinCube(moduleName)
//          kylinClient.refreshOrBuildCubeAtLeastOnceTask(cubes, lastCommitTime)
//        }

        lastCommitTime = new Date()
        //      }

        transactionManager.commitTransaction(true, moduleName, {})
        moduleTracer.trace("transaction commit ")
//        traceBatchUsingTime("transaction manager commit", lastTraceTime, traceBatchUsingTimeLog)

        //uuidT, dwrT, dwiT, kafkaT,
        if (isEnableDwiUuid) {
          hbaseClient.clean(uuidT)
          moduleTracer.trace("clean hbase uuid")
//          traceBatchUsingTime("clean hbase uuid", lastTraceTime, traceBatchUsingTimeLog)
        }
        if (isEnableDwr) {
          hiveClient.clean(dwrT)
          moduleTracer.trace("clean hive dwr")
//          traceBatchUsingTime("clean hive dwr", lastTraceTime, traceBatchUsingTimeLog)
        }
        if (isEnableDwi) {
          hiveClient.clean(dwiT)
          moduleTracer.trace("clean hive dwi")
//          traceBatchUsingTime("clean hive dwi", lastTraceTime, traceBatchUsingTimeLog)
        }
        if(dwiExtendedFieldsHandlerCookies != null) {
          dwiExtendedFieldsHandlerCookies.foreach{ x=>
            x._2.foreach{y=>
              x._1.clean(y)
              moduleTracer.trace("clean dwi extended handler")
//              traceBatchUsingTime("clean dwi extended handler", lastTraceTime, traceBatchUsingTimeLog)
            }
          }
        }


        kafkaClient.clean(kafkaT)
        moduleTracer.trace("clean kafka")
//        traceBatchUsingTime("clean kafka", lastTraceTime, traceBatchUsingTimeLog)

        if (isEnablePhoenixDm) phoenixClient.clean(dmT)

        //Reset
        dwiExtendedFieldsHandlerCookies = Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()

        //      transactionManager.clean(tid)
//        traceBatchUsingTime("clean", lastTraceTime, traceBatchUsingTimeLog)
      }

      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      moduleTracer.trace("set module status")
//      traceBatchUsingTime("set module status", lastTraceTime, traceBatchUsingTimeLog)

      moduleTracer.endBatch()
//      batchUsingTime = batchUsingTime + ( new Date().getTime - batchBeginTime)
      mySqlJDBCClient.execute(
        s"""
           |  insert into module_running_status(
           |    app_name,
           |    module_name,
           |    batch_using_time,
           |    batch_actual_time,
           |    update_time,
           |    rebrush,
           |    batch_using_time_trace
           |  )
           |  values(
           |    "$appName",
           |    "$moduleName",
           |    ${moduleTracer.getBatchUsingTime()},
           |    ${moduleTracer.getBatchActualTime()},
           |    now(),
           |    "${argsConfig.get(ArgsConfig.REBRUSH, "NA").toUpperCase}",
           |    "${moduleTracer.getTraceResult()}"
           |  )
           |  on duplicate key update
           |    app_name = values(app_name),
           |    batch_using_time = values(batch_using_time),
           |    batch_actual_time = values(batch_actual_time),
           |
           |    update_time = values(update_time),
           |    rebrush = values(rebrush),
           |    batch_using_time_trace = values(batch_using_time_trace)
           """.stripMargin)

      }catch {
        case e:Exception=> throw new ModuleException(s"${classOf[GenericModule].getSimpleName} '$moduleName' execution failed !! ", e)
      }
    }
  }

  def upsertGreenplum(dwiT:TransactionCookie, dwrT:TransactionCookie): Unit ={
    dmGreenplumUpsertByHiveViews.map{x=>
      var createViewSQL = hiveClient.sql(s"show create table ${x._1}").first().getAs[String]("createtab_stmt")

      var parts = Array[HivePartitionPart]()
      val dmV = s"${x._1}_4_upsert"
      createViewSQL = createViewSQL.replaceAll("CREATE VIEW", "CREATE OR REPLACE TEMPORARY VIEW").replaceAll(x._1, dmV)

      if(StringUtil.notEmpty(x._2)) {
        val dwiV= s"${x._2}_4_upsert"
        cacheDwi.selectExpr("*", s"cast(to_date($businessDateExtractBy) as string)  as b_date").createOrReplaceTempView(dwiV)
        createViewSQL = createViewSQL.replaceAll(s"`default`.`${x._2}`", dwiV)
        parts ++= dwiT.asInstanceOf[HiveRollbackableTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct
      }
      if(StringUtil.notEmpty(x._3)) {
        val dwrV = s"${x._3}_4_upsert"
        cacheGroupByDwr.createOrReplaceTempView(dwrV)
        createViewSQL = createViewSQL.replaceAll(s"`default`.`${x._3}`", dwrV)
        parts ++= dwrT.asInstanceOf[HiveRollbackableTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct
      }

      hiveClient.sql(createViewSQL)

      greenplumClient.upsert(x._1, hiveContext.read.table(dmV), x._4, parts, "b_date")

      if(parts.length > 0) moduleTracer.trace(s"gp upsert $x")
    }
  }
  def overwrideGreenplum (): Unit ={
    val c = "GreenplumConsumer"
    val ts = dmGreenplumMessageTopicsExt:+moduleName

    val pd = messageClient
      .pullMessage(new MessagePullReq(c, ts))
      .getPageData

    val ps = pd
      .map{x=>
        OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
      }
      .flatMap{x=>x}
      .flatMap{x=>x}
      .filter{x=>"b_date".equals(x.name)}
      .distinct
      .toArray

    dmGreenplumHiveViews.foreach{x=>
      greenplumClient.overwrite(
        x,
        x,
        ps,
        "b_date");
      if(ps.length>0) moduleTracer.trace(s"gp overwrite $x")
    }

    messageClient.commitMessageConsumer(
      pd.map {x=>
        new MessageConsumerCommitReq(c, x.getTopic, x.getOffset)
      }:_*
    )
  }

  //  def updateKylinCube (kylinCubes:util.List[String], moduleName: String): Unit ={
  //    try{
  //      val c = Calendar.getInstance
  //      c.setTime(lastCommitTime);
  //
  //      c.set(Calendar.HOUR,0)
  //      c.set(Calendar.MINUTE, 0)
  //      c.set(Calendar.SECOND,0)
  //      c.set(Calendar.MILLISECOND, 0)
  //      val st = c.getTime
  //      c.store(Calendar.DATE, 1)
  //      val et = c.getTime
  //
  //      if(cubes != null) {
  //        cubes.foreach{x=>
  //          kylinClient.refreshOrBuildCubeAtLeastOnceTask(x, st, et)
  //        }
  //      }
  //    }catch {
  //      case e:Exception=> LOG.warn("kylinClient updateKylinCube fail", e)
  //    }
  //
  ////    kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_traffic_overall_dm_final", st, et)
  ////    kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_user_keep_dm", st, et)
  ////    kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_user_na_dm", st, et)
  ////    kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_traffic_app_dm", st, et)
  ////    kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_traffic_campaign_dm", st, et)
  //  }

  private def pushMessage (dwrT:HiveRollbackableTransactionCookie, dwiT:HiveRollbackableTransactionCookie): Unit ={
    var ps:Array[Array[HivePartitionPart]] = null
    if(dwrT != null) {
      ps = dwrT.partitions
    }
    if(dwiT != null && ps == null) {
      ps = dwiT.partitions
    }
    if(ps != null) {
      val sd = ps.map{x=>
        x.sortBy{y=> y.name + y.value}
      }.sortBy{x=> OM.toJOSN(x)}
      val topic = moduleName
      val key = OM.toJOSN(sd)
      val uniqueKey = true
      val data = ""
      messageClient.pushMessage(new MessagePushReq(topic, key, uniqueKey, data))
      LOG.warn(s"MessageClient pushMessage completed", s"topic: $moduleName, \nkey: $key, \nuniqueKey: $uniqueKey, \ndata: $data")
    }else {
      LOG.warn(s"MessageClient pushMessage nothing!")
    }
  }

  def stop (): Unit = {
    try {
      if (stream != null) stream.stop()
    } catch {
      case e: Exception =>
        LOG.error(s"${getClass.getName} '$moduleName' stop fail!", e)
    }
  }


}
