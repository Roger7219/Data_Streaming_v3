package com.mobikok.ssp.data.streaming.module

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.Date

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.protobuf.Message
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.AS3
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, OffsetRange, UuidStat}
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.module.support._
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/6/8.
  */
class MixModule (config: Config,
                 argsConfig: ArgsConfig,
                 concurrentGroup: String,
                 mixModulesBatchController: MixModulesBatchController,
                 moduleName: String,
                 runnableModuleNames: Array[String],
                 /* dwiStructType: StructType,*/
                 ssc: StreamingContext) extends Module {

  //  private[this] val LOGGER = Logger.getLogger(getClass().getName());
  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  val appName = ssc.sparkContext.getConf.get("spark.app.name")
  val ksc = Class.forName(config.getString(s"modules.${moduleName}.dwi.kafka.schema"))
  val dwiStructType = ksc.getMethod("structType").invoke(ksc.newInstance()).asInstanceOf[StructType]

  val dwiUuidFieldsSeparator = "^"

  //moduleName, transactionCookies

  val COOKIE_KIND_UUID_T = "uuidT"
  val COOKIE_KIND_DWI_EXT_FIELDS_T = "dwiExtFieldsT"
  val COOKIE_KIND_DWI_T = "dwiT"
  val COOKIE_KIND_DWR_T = "dwrT"
  //  val COOKIE_KIND_DWI_PHOENIX_T = "dwiPhoenixT"
  val COOKIE_KIND_KAFKA_T = "kafkaT"
  val COOKIE_KIND_DM_T = "dmT"
  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

  //<cookieKind, <transactionParentId, transactionCookies>>
  var batchsTransactionCookiesCache = new util.HashMap[String, util.ArrayList[TransactionCookie]]()//mutable.Map[String, ListBuffer[TransactionCookie]]()

  //Value Type: String or Array[Byte]
  var stream:InputDStream[ConsumerRecord[String, Object]] = null

  val hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
  val sqlContext = SQLContextGenerater.generate(ssc.sparkContext)
  var kylinClient: KylinClientV2 = null
  try {
    kylinClient = new KylinClientV2(config.getString("kylin.client.url"))
  }catch {
    case e:Exception=> LOG.warn("KylinClient init fail !!", e.getMessage)
  }

  //val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
  //val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //  var mySqlJDBCClient = new MySqlJDBCClient(
  //    config.getString(s"rdb.url"),
  //    config.getString(s"rdb.user"),
  //    config.getString(s"rdb.password")
  //  )
//  var mySqlJDBCClient = new MySqlJDBCClient(
//    config.getString(s"rdb.url"),
//    config.getString(s"rdb.user"),
//    config.getString(s"rdb.password")
//  )

  var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    moduleName, config.getString(s"rdb.url"), config.getString(s"rdb.user"), config.getString(s"rdb.password")
  )

  //  val greenplumJDBCClient = new GreenplumJDBCClient(
  //    moduleName,
  //    "jdbc:pivotal:greenplum://node15:5432;DatabaseName=postgres",
  //    "gpadmin",
  //    "gpadmin"
  //  )
  var moduleTracer: ModuleTracer = new ModuleTracer(moduleName, config, mixModulesBatchController)
  var greenplumClient = null.asInstanceOf[GreenplumClient]
  try {
    greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  }catch {case e:NoClassDefFoundError => LOG.warn("GreenplumClient no class found", e.getMessage)}

  var bigQueryClient = null.asInstanceOf[BigQueryClient]
  try{
    bigQueryClient = new BigQueryClient(moduleName, config, ssc, messageClient, hiveContext)
  }catch {case e:Throwable => LOG.warn("BigQueryClient init fail, Skiped it", e.getMessage)}

  var clickHouseClient = null.asInstanceOf[ClickHouseClient]
  try {
    clickHouseClient = new ClickHouseClient(moduleName, config, ssc, messageClient, mixTransactionManager, hiveContext, moduleTracer)
  } catch {case e: Exception => LOG.warn(s"Init clickhouse client failed, skip it, ${e.getMessage}")}

  val rDBConfig = new RDBConfig(mySqlJDBCClientV2)

  var dwiPhoenixEnable = false
  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable = null.asInstanceOf[String]
  var dwiPhoenixHBaseStorableClass = null.asInstanceOf[String]
  try {
    dwiPhoenixEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.enable")
  }catch {case e:Exception=>}
  if(dwiPhoenixEnable) {
    dwiPhoenixTable = config.getString(s"modules.$moduleName.dwi.phoenix.table")
    dwiPhoenixHBaseStorableClass = config.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
  }
  if(config.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")) {
    dwiPhoenixSubtableEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
  }

  //  val transactionManager: TransactionManager = new TransactionManager(config)
  val mixTransactionManager = mixModulesBatchController.getMixTransactionManager()

  val messageClient = new MessageClient(moduleName, config.getString("message.client.url"))
  var hbaseClient:HBaseMultiSubTableClient = null //new HBaseClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
//  if(dwiPhoenixEnable) {
    hbaseClient = new HBaseMultiSubTableClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
//  }
  val hiveClient = new HiveClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, config, mixTransactionManager)
  val phoenixClient = new PhoenixClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)

  val monitorclient = new MonitorClient(messageClient);
  val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS")

  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(config.getString(s"modules.$moduleName.dm.load.time.format.by"))
  }catch { case _: Exception =>}

  //  //val dwrFirstLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //  var dwiLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
  //  try {
  //    dwiLoadTimeFormat = new SimpleDateFormat(config.getString(s"modules.$moduleName.dwi.load.time.format.by"))
  //  }catch { case _: Exception =>}
  //  if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
  //    dwiLoadTimeFormat = new SimpleDateFormat(dwiLoadTimeFormat.toPattern.substring(0, 17) + "01")
  //  }
  //
  //  var dwrLoadTimeFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  //  try {
  //    dwrLoadTimeFormat = new SimpleDateFormat(config.getString(s"modules.$moduleName.dwr.load.time.format.by"))
  //  }catch {case _:Exception => }
  //  if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
  //    dwrLoadTimeFormat = new SimpleDateFormat(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")
  //  }

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

  var commitBatchSize = 0
  try {
    commitBatchSize = config.getInt(s"modules.$moduleName.commit.batch.size")
  }catch {case e:Exception=>}


  //val dwrRollBatchSize = config.getInt(s"modules.$moduleName.dwr.roll.batch.size")
  var lastCommitTime = new Date();
  var commitTimeInterval = 0
  try {
    commitTimeInterval = config.getInt(s"modules.$moduleName.commit.time.interval")
  }catch {case e:Exception=>}

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

  var dwiHandlers:List[(java.util.List[String], Column, com.mobikok.ssp.data.streaming.handler.dwi.Handler)]  = null

//  var dwiBTimeFormat = "yyyy-MM-dd 00:00:00"
//  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
//  try {
//    dwiBTimeFormat = config.getString(s"modules.$moduleName.dwi.business.time.format.by")
//  }catch {case e: Throwable=>}
//  try {
//    dwrBTimeFormat = config.getString(s"modules.$moduleName.dwr.business.time.format.by")
//  }catch {case e: Throwable=>}

  var businessTimeExtractBy: String = null
  try {
    businessTimeExtractBy = config.getString(s"modules.$moduleName.business.time.extract.by")
  }catch {case e:Throwable=>
    //兼容历史版本配置
    businessTimeExtractBy = config.getString(s"modules.$moduleName.business.date.extract.by")
  }

  val topics = config.getConfigList(s"modules.$moduleName.kafka.consumer.partitoins").map { x => x.getString("topic") }.toArray[String]

  //==
  var dwrGroupbyExtendedFields:List[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = null
  try {
    dwrGroupbyExtendedFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.extended.fields").map{ x=>
      val hc = x.getConfig("handler")
      var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
      h.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, x.getString("expr"), x.getString("as"))
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
      h.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, x.getString("expr"), x.getString("as"))
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
  var dmHandlers: util.List[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler] = null
  try {
    isEnableHandlerDm = config.getBoolean(s"modules.$moduleName.dm.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableHandlerDm) {
    dmHandlers = new util.ArrayList[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]()
    config.getConfigList(s"modules.$moduleName.dm.handler.setting").foreach { x =>
      var h = Class.forName(x.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]
      h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient,kylinClient, hbaseClient, hiveContext, x)
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
  try{
    dwiTable = config.getString(s"modules.$moduleName.dwi.table")
  }catch {case e:Throwable=>
    if(isEnableDwi) throw e
  }

  var isEnableDwr = false

  var isMaster = mixModulesBatchController.isMaster(moduleName)
//  override def isMaster(): Boolean = {
//    shareDwrTableModulesBatchController.isMaster(moduleName)
//  }
//
//  override def getName(): String = moduleName
//  override def isInitable(): Boolean = {
//    var result = false
//    val rms = config.root.map(_._1)
//
//    if(rms.contains(moduleName)) {
//      result = true
//    } else {
//      rms.foreach{moduleName =>
//        if(!result) {
//          val sms = mixTransactionManager.prevRunningSameTransactionGroupModules(moduleName)
//          result = sms.contains(moduleName)
//        }
//      }
//    }
//    result
//  }
//  override def isRunnable(): Boolean = {
//    runnableModuleNames.contains(moduleName)
//  }

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
  if(config.hasPath(s"modules.$moduleName.dwr.include.repeated")) {
    dwrIncludeRepeated = config.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  }else if(isEnableDwiUuid) {
    dwrIncludeRepeated = false
  }

  var isFastPollingEnable = false
  if(config.hasPath(s"modules.$moduleName.fast.polling.enable")) {
    isFastPollingEnable = config.getBoolean(s"modules.$moduleName.fast.polling.enable")
  }

  //  @volatile var cacheGroupByDwr: DataFrame = null
  //对于没有重复的数据hbase repeats:1, hive repeats :0
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


  //  var uuidBloomFilter: BloomFilter = null
  //

  def initDwiHandlers(): Unit ={
    if(config.hasPath(s"modules.$moduleName.dwi.handler")) {
      dwiHandlers = config.getConfigList(s"modules.$moduleName.dwi.handler").map { x =>
        val hc = x.getConfig("handler")
        var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
        var col = "null"
        var as:java.util.List[String] = new util.ArrayList[String]()

        try {
          if(StringUtil.notEmpty(x.getString("expr"))) col = x.getString("expr")
        } catch {case ex:Throwable=>}

        try {
          as =  x.getStringList("as")
        }catch {case ex:Throwable=>}

        h.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, hc, config, col/*x.getString("expr")*/, as.toArray( Array[String]() ))
        (as, expr(col), h)
      }.toList
    }

  }

  @volatile var hiveCleanable, hbaseCleanable, kafkaCleanable, phoenixCleanable: Cleanable = null
  override def init (): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} init started", moduleName)
      //      AS3.main3(sqlContext)
      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      hiveClient.init()
      if(hbaseClient != null) hbaseClient.init()
      kafkaClient.init()
      phoenixClient.init()

      initDwiHandlers()

      hiveCleanable = hiveClient.rollback()
      if(hbaseClient != null) hbaseCleanable = hbaseClient.rollback()
      kafkaCleanable = kafkaClient.rollback()
      phoenixCleanable = phoenixClient.rollback()

      mySqlJDBCClientV2.execute(
        s"""
           |  insert into module_running_status(
           |    app_name,
           |    module_name,
           |    update_time,
           |    rebrush,
           |    batch_buration,
           |    batch_using_time,
           |    batch_actual_time
           |  )
           |  values(
           |    "$appName",
           |    "$moduleName",
           |    now(),
           |    "${argsConfig.get(ArgsConfig.REBRUSH, "NA").toUpperCase}",
           |    ${(100.0*config.getInt("spark.conf.streaming.batch.buration")/60).asInstanceOf[Int]/100.0},
           |    null,
           |    null
           |  )
           |  on duplicate key update
           |    app_name = values(app_name),
           |    update_time = values(update_time),
           |    rebrush = values(rebrush),
           |    batch_buration = values(batch_buration),
           |    batch_using_time = null,
           |    batch_actual_time = null
           """.stripMargin)

      //      if(isEnableDwiUuid) {
      //
      //      }

      LOG.warn(s"${getClass.getSimpleName} init completed", moduleName)
    } catch {
      case e: Exception =>
        throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' init fail, Transactionals rollback exception", e)
    }
  }

  var uuidBloomFilterMap:util.Map[String, BloomFilterWrapper] = new util.HashMap[String, BloomFilterWrapper]()
  var bloomFilteBTimeformat = "yyyy-MM-dd HH:00:00"
  def filterRepeatedUuids(ids: Array[(String, Iterable[(String, String)])] /*Array[String]*/): DataFrame ={

    //Uuid BloomFilter 去重（重复的rowkey）
    val repeatedIds = ids.map{case(bt, _ids)=>

      val bf = new BloomFilter(math.max(20,/*(Int.MaxValue/100000000)*/ 20 * _ids.count(x=> x._2 != null)), 12 /*16*/, Hash.MURMUR_HASH)
      var reps = _ids.filter{y=>
        val z = y._2
        var re = false
        if(z == null || z.length == 0){
          re = false
        }else {
          val f = uuidBloomFilterMap.get(bt)

          val k = new Key(z.getBytes())

          if(f.membershipTest(k)) {
            LOG.warn("filterRepeatedUuids：data has repeated" , "repeated id: ",z)//test
            re = true
          }else {

            bf.add(k)
          }
        }
        re
      }
      //去重后的数据加入当前小时bf中
      var wrap = uuidBloomFilterMap.get(bt)
      if(wrap == null) {
        wrap = new BloomFilterWrapper()
        uuidBloomFilterMap.put(bt, wrap)
      }
      wrap.append(bf)

      reps

    }.flatMap{x=>x}.map(_._1)

    //拓展,对当前批次生成对应的BF(前批次去重后的数据)
    /*ids.foreach{case(bt, _ids)=>
      LOG.warn("filterRepeatedUuids：BloomFilter" , "take 4 ids", _ids.map(x=>x._2).take(4).mkString("[", ",", "]"), "add id count: ", _ids.count(x=> x._2 != null))//test
      val deRepeateds = _ids.filter()

      val bf = new BloomFilter(math.max(20,/*(Int.MaxValue/100000000)*/ 20 * _ids.count(x=> x._2 != null)), 12 /*16*/, Hash.MURMUR_HASH)

      _ids.foreach{x=>
        bf.add(new Key(x._2.getBytes))
      }
      var wrap = uuidBloomFilterMap.get(bt)
      if(wrap == null) {
        wrap = new BloomFilterWrapper()
        uuidBloomFilterMap.put(bt, wrap)
      }
      wrap.append(bf)
    }*/

    val res = hiveContext.createDataFrame(repeatedIds.map{x=> new UuidStat(x, 1)})
    res
  }


  var bloomFilterRDDs:Array[RDD[(String, BloomFilter)]] = new Array[RDD[(String, BloomFilter)]](0)//sqlContext.sparkContext.parallelize(Seq[(String, BloomFilter)]())
  //sqlC.sparkContext.parallelize(Seq( ("2", new BloomFilter(Integer.MAX_VALUE, 16,1)) ))

  def loadUuidsIfNonExists(dwiTable: String, b_dates:Array[(String, String)]): Unit = {
    LOG.warn("BloomFilter try load uuids if not exists start", s"contained b_date: ${OM.toJOSN(uuidBloomFilterMap.keySet())}\ndwi table: $dwiTable\ntry apppend b_dates: ${OM.toJOSN(b_dates)}")

    b_dates.foreach{case(b_date, b_time)=>

      var f = uuidBloomFilterMap.get(b_time)
      if(f == null) {
        LOG.warn("BloomFilter load uuids start", "b_date", b_date, "b_time", b_time)

//        f = new BloomFilter(Integer.MAX_VALUE, 16, Hash.MURMUR_HASH);
//        uuidBloomFilterMap.put(b_time, f)

        //Spark序列化
        val a = dwiUuidFieldsAlias
        var c: Array[Array[Byte]] = null
        RunAgainIfError.run({
          c = hiveContext
            .read
            .table(dwiTable)
            .select(col(a))
            .where(s"repeated = 'N' and b_date = '${b_date}' and from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilteBTimeformat}') = '${b_time}' ")
            .rdd
            .map{x=>
              var id = x.getAs[String](a)
              if(id != null) id.getBytes() else null
            }
            .collect()
        })
        LOG.warn("read dwi table uuids done", "count ", c.length, "b_time ", b_time)

        val bf = new BloomFilter(math.max(20,/*(Int.MaxValue/100000000)*/ 20 * c.length), 12 /*16*/, Hash.MURMUR_HASH)
        var wrap = uuidBloomFilterMap.get(b_time)
        if(wrap == null) {
          wrap = new BloomFilterWrapper()
          uuidBloomFilterMap.put(b_time, wrap)
        }
        wrap.append(bf)

        c.foreach{x=>
          if(x != null && x.length > 0) {
            bf.add(new Key(x))
          }
        }

        LOG.warn("read dwi table uuids done, count", c.length)

//          c.take(1).foreach{x=>
//            LOG.warn("BloomFilter test uuids filter by", x)
//            LOG.warn("BloomFilter test uuids filter result", f.membershipTest(new Key(x)))
//          }
      }

    }

    //清除掉不用的，释放内存
    import scala.collection.JavaConverters._
    uuidBloomFilterMap = uuidBloomFilterMap.filter{case(bt, _)=>
      b_dates.map(_._2).contains(bt)
    }.asJava

    if(b_dates.size > 0) {
      moduleTracer.trace("    read dwi table uuids")
    }

    LOG.warn("BloomFilter try load uuids if not exists done")
  }

  def start (): Unit = {

  }

  var dwiExtendedFieldsHandlerCookies: Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])] = Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()

  val traceBatchUsingTimeFormat = new DecimalFormat("#####0.00")

  @volatile var moduleReadingKafkaMarks = mutable.Map[String, Boolean]()

  def handler (): Unit = {
    //
    //    val lock = new Object
    //    lock.synchronized{
    //      new Thread(new Runnable {
    //        override def run (): Unit = {
    //          lock.synchronized{
    stream = kafkaClient.createDirectStream(config, ssc, kafkaClient.getCommitedOffset(topics: _*), moduleName)
    //            lock.notifyAll()
    //          }
    //        }
    //      }).start()
    //
    //      lock.wait()
    //    }

    //    // kafkaClient.createDirectStream(topics(0), config, ssc, moduleName)
    //    stream.start()

    stream.foreachRDD { source =>
      try {

        moduleTracer.startBatch

        val offsetRanges = source
          .asInstanceOf[HasOffsetRanges]
          .offsetRanges
          .map { x =>
            OffsetRange(x.topic, x.partition, x.fromOffset, x.untilOffset)
          }

        val offsetDetail: Iterable[(TopicAndPartition, Long, Long)] = kafkaClient
          .getLatestOffset(offsetRanges.map { x => x.topic })
          .map { x =>
            val v = offsetRanges.filter(o => o.topic.equals(x._1.topic) && o.partition.equals(x._1.partition))
            // topic, partition, cnt, lag
            if(v.isEmpty) {
              (x._1, 0.asInstanceOf[Long], 0.asInstanceOf[Long])
            }else {
              (x._1, v(0).untilOffset - v(0).fromOffset, x._2 - v(0).untilOffset)
            }
          }

        var kafkaConsumeLag = 0.asInstanceOf[Long]
        offsetDetail.foreach{x=>
          kafkaConsumeLag = kafkaConsumeLag + x._3
        }

        moduleTracer.trace(s"offset detail cnt: ${offsetDetail.map(_._2).reduce(_+_)} lag: ${offsetDetail.map(_._3).reduce(_+_)}\n" + offsetDetail.map { x => s"        ${x._1.topic}, ${x._1.partition} -> cnt: ${x._2} lag: ${x._3}" }.mkString("\n"))

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

        if (isFastPollingEnable && filtered.isEmpty() && GlobalAppRunningStatusV2.isPreviousRunning(concurrentGroup, moduleName)) {
          LOG.warn("Fast polling", "concurrentGroup", concurrentGroup, "moduleName", moduleName)
          moduleTracer.trace("fast polling")
        }else {
          //-----------------------------------------------------------------------------------------------------------------
          // Checking concurrent group status START
          //-----------------------------------------------------------------------------------------------------------------
          moduleTracer.pauseBatch()
          //      batchUsingTime = batchUsingTime + (new Date().getTime - batchBeginTime)
          LOG.warn(s"Checking global app running status [ $concurrentGroup, $moduleName ]", GlobalAppRunningStatusV2.allStatusToString)
          GlobalAppRunningStatusV2.waitRunAndSetRunningStatus(concurrentGroup, moduleName)

          moduleTracer.trace("wait module queue")
          moduleTracer.continueBatch()
          //-----------------------------------------------------------------------------------------------------------------
          // Checking concurrent group status END
          //-----------------------------------------------------------------------------------------------------------------

          val groupName = if(StringUtil.notEmpty(dwrTable)) dwrTable else moduleName
          val parentTid = mixTransactionManager.beginTransaction(moduleName, groupName)

          //      batchBeginTime = new Date().getTime
          //      lastTraceTime.update(0, new Date().getTime)

          var dwi = hiveContext.read
            .schema(dwiStructType)
            .json(filtered)

          moduleTracer.trace("generate dwi df")

          // DWI Extended Fields Handler
          if(dwiHandlers != null) {
            dwiHandlers.foreach { x =>
              //Key code !!
              val y = x._3.handle(dwi)
              //切断rdd依赖
              dwi = hiveContext.createDataFrame(y._1.collectAsList(), y._1.schema)
              dwiExtendedFieldsHandlerCookies :+= (x._3, y._2)
            }
            moduleTracer.trace("dwi handler")
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

            moduleTracer.trace("dwi left join")
          }

  //        if(isEnableDwi || isEnableDwr || isEnableDwiUuid) {
          dwi.persist(StorageLevel.MEMORY_ONLY_SER)
  //        }

          // Statistical repeats
          // For spark serializable
          var _uuidF = this.dwiUuidFieldsAlias

          var saved:DataFrame = null

          if (isEnableDwiUuid) {
            if (cacheUuidRepeats != null) {

              var ids = dwi
                .dropDuplicates(dwiUuidFieldsAlias)
                .alias("x")
                .join(cacheUuidRepeats, col(s"x.$dwiUuidFieldsAlias") === col("ur.uuid"), "left_outer")
                .where("ur.uuid is null")
                .select(
                  expr(s"x.$dwiUuidFieldsAlias"),
                  expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time")
                )
                .rdd
                .map { x =>
                  (x.getAs[String]("b_time"), x.getAs[String](_uuidF) )
                }
                .groupBy(_._1)
                .collect()

              moduleTracer.trace("    generate uuids")
              LOG.warn("cacheUuidRepeats!=null, Filter cache uuid take(2)", if(ids.nonEmpty) s"${ids.head._1} : ${ids.head._2.take(2)}" else "")

              //            saved = repeatedCount(dwi).alias("saved")
              var b_dates = dwi
                .select(
                  to_date(expr(businessTimeExtractBy)).cast("string").as("b_date"),
                  expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time")
                )
                .dropDuplicates("b_date", "b_time")
                .rdd.map{
                x=> (
                  x.getAs[String]("b_date"),
                  x.getAs[String]("b_time")
                )
              }.collect()
              loadUuidsIfNonExists(dwiTable, b_dates)
              val saved = filterRepeatedUuids(ids).alias("saved")
              //
              ////            val saved = hbaseClient.getsAsDF(dwiUuidStatHbaseTable, ids, classOf[UuidStat]).alias("saved")

              moduleTracer.trace("    get saved uuid repeats")
              LOG.warn("cacheUuidRepeats not null, Get saved", s"count: ${saved.count()}\ntake(2): ${util.Arrays.deepToString(saved.take(2).asInstanceOf[Array[Object]])}" )


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

              cacheUuidRepeats.persist(StorageLevel.MEMORY_ONLY_SER)
              //            cacheUuidRepeats = hiveContext.createDataFrame(cacheUuidRepeats.collectAsList(), cacheUuidRepeats.schema).repartition(shufflePartitions).alias("ur")

              LOG.warn("cacheUuidRepeats not null, cacheUuidRepeats", s"count:${cacheUuidRepeats.count()}\ntake(2): ${util.Arrays.deepToString(cacheUuidRepeats.take(2).asInstanceOf[Array[Object]])}")

            } else {

              var ids = dwi
                .dropDuplicates(dwiUuidFieldsAlias)
                .select(
                  expr(s"$dwiUuidFieldsAlias"),
                  expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time")
                )
                .rdd
                .map { x =>
                  (x.getAs[String]("b_time"), x.getAs[String](_uuidF) )
                }
                .groupBy(_._1)
                .collect()

              moduleTracer.trace("    generate uuids")
              LOG.warn("cacheUuidRepeats is null, dwi uuid take(5)", if(ids.nonEmpty) s"${ids.head._1} : ${ids.head._2.take(2)}" else "")

              //            var ids = dwi
              //              .select(
              //                to_date(expr(businessDateExtractBy)).as("b_date"),
              //                col(dwiUuidFieldsAlias)
              //              )
              //              .dropDuplicates("b_date", dwiUuidFieldsAlias)
              //              .rdd
              //              .collect()
              //            filterRepeatedUuids(ids)

              //            saved = repeatedCount(dwi).alias("saved")

              var b_dates = dwi
                .select(
                  to_date(expr(businessTimeExtractBy)).cast("string").as("b_date"),
                  expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time")
                )
                .dropDuplicates("b_date", "b_time")
                .rdd.map{
                x=> (
                  x.getAs[String]("b_date"),
                  x.getAs[String]("b_time")
                )
              }.collect()
              loadUuidsIfNonExists(dwiTable, b_dates)
              val saved = filterRepeatedUuids(ids).alias("saved")
              //
              ////            val saved = hbaseClient.getsAsDF(dwiUuidStatHbaseTable, ids, classOf[UuidStat]).alias("saved")

              moduleTracer.trace("    get saved uuid repeats")
              LOG.warn("cacheUuidRepeats is null, Get saved", s"count: ${saved.count()}\ntake(2): ${util.Arrays.deepToString(saved.take(2).asInstanceOf[Array[Object]])}" )

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
                .alias("ur")

              // ???           cacheUuidRepeats = hiveContext.createDataFrame(cacheUuidRepeats.collectAsList(), cacheUuidRepeats.schema).repartition(shufflePartitions).alias("ur")
              cacheUuidRepeats.persist(StorageLevel.MEMORY_ONLY_SER)

              LOG.warn("cacheUuidRepeats is null, cacheUuidRepeats", s"count: ${cacheUuidRepeats.count()}\ntake(2): ${util.Arrays.deepToString(cacheUuidRepeats.take(2).asInstanceOf[Array[Object]])}")

              //cacheUuidRepeats.collect()
            }

            moduleTracer.trace("uuid repeats stat")
            //        traceBatchUsingTime("uuid repeats stat", lastTraceTime, traceBatchUsingTimeLog)
          }

          //----------------------------------------------------------------------------------------------------------------
          //整合离线统计 START
          //----------------------------------------------------------------------------------------------------------------
          var firstDwiLTime:String = null
          var firstDwrLTime:String = null
          //        val cd = DateTimeUtil.uniqueSecondes()
          firstDwiLTime = mixTransactionManager.dwiLoadTime()//dwiLoadTimeFormat.format(cd)
          firstDwrLTime = mixTransactionManager.dwrLoadTime()//dwrLoadTimeFormat.format(cd)
          //var dwiLTimeSourceField, dwrLTimeSourceField:String = "null"
  //        try{
  //          if(dwi.schema.fieldIndex("dwrLoadTime") >= 0) {
  //            dwiLTimeSourceField = "dwrLoadTime"
  //            dwrLTimeSourceField = "dwrLoadTime"
  //          }
  //        }catch {case  e:Exception=>}

          //-1.关闭流处理，启动流处理，启动离线统计
          //0.检测offline.rebrush.data.running是否为true
          //1.数据源头层为新流入数据的l_time分区值 + 1秒

          //date_format( nvl(null, '2017-09-12 00:00:00'), 'yyyy-MM-dd 00:00:00')
          var dwiLTimeExpr, dwrLTimeExpr = null.asInstanceOf[String]

          dwiLTimeExpr = s"'$firstDwiLTime'"
          dwrLTimeExpr = s"'$firstDwrLTime'"
          //兼顾事务模型，不得不放弃下面的方式
          //        if("null".equals(dwiLTimeSourceField) ) {
          //          dwiLTimeExpr = s"'$firstDwiLTime'"
          //          dwrLTimeExpr = s"'$firstDwrLTime'"
          //        }else {
          //          dwiLTimeExpr = s"date_format($dwiLTimeSourceField, '${dwiLoadTimeFormat.toPattern}')"
          //          dwrLTimeExpr = s"date_format($dwrLTimeSourceField, '${dwrLoadTimeFormat.toPattern}')"
          //        }
          LOG.warn("dwiLTimeExpr, dwrLTimeExpr", s"$dwiLTimeExpr, $dwrLTimeExpr" );

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

          //新流入的数据写到l_time+1s的分区，不属于+1s的分区就忽略掉
          if(isExcludeOfflineRebrushPart) {
            val w = s"second(l_time) > 0 "
            newDwi = newDwi.where(w)
            LOG.warn("newDwi filter by ", w)
          }
          //----------------------------------------------------------------------------------------------------------------
          //整合离线统计 END
          //----------------------------------------------------------------------------------------------------------------

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
              .withColumn("b_date", to_date(expr(businessTimeExtractBy)).cast("string"))
              .groupBy(col("l_time") :: col("b_date") :: dwrGroupByExprs: _*)
              //.groupBy("b_date", dwrGroupByExprs: _*)
              .agg(/*count(lit(1)).alias("times") ,*/ aggExprs.head, aggExprs.tail: _*)
            //          .withColumn("fl_time", lit(null))
            // ???         dwr = hiveContext.createDataFrame(dwr.collectAsList(), dwr.schema).repartition(shufflePartitions)
  //          dwr.persist(StorageLevel.MEMORY_ONLY_SER)

  //          if(isEnableDwr) LOG.warn("New dwr take(2)", dwr.take(2))

            moduleTracer.trace("generate dwr df")
            moduleTracer.pauseBatch()

            mixModulesBatchController.waitUnionAll(dwr, isMaster, moduleName)
            if(isEnableDwr && isMaster) LOG.warn("master threadSafeDwr take(2)", mixModulesBatchController.get().take(2))

            moduleTracer.trace("dwr wait union all")
            moduleTracer.continueBatch()

            moduleTracer.trace("refresh cacheDwr")
            //        traceBatchUsingTime("refresh cacheDwr", lastTraceTime, traceBatchUsingTimeLog)


            if(dwrGroupbyExtendedFields != null && dwrGroupbyExtendedFields.size > 0) {
              if(!isMaster) {
                throw new ModuleException("Module of include 'dwr.groupby.extended.fields' must config: master=true")
              }
              dwrGroupbyExtendedFields.foreach{ x=>
                mixModulesBatchController.set({x.handle(mixModulesBatchController.get())})
                //              cacheGroupByDwr = x.handle(cacheGroupByDwr)
                //              cacheGroupByDwr = hiveContext.createDataFrame(cacheGroupByDwr.collectAsList(), cacheGroupByDwr.schema).repartition(shufflePartitions)
              }
              LOG.warn("hiveClient dwrExtendedFields all handlers completed, take(2)", mixModulesBatchController.get().take(2))
              moduleTracer.trace("dwr extended fields handler")
              //            traceBatchUsingTime("dwr extended fields handler", lastTraceTime, traceBatchUsingTimeLog)
            }

            if(dwrAfterFilter != null) {
              if(!isMaster) {
                throw new ModuleException("Module of include 'dwr.filter.after' must config: master=true")
              }
              mixModulesBatchController.set({mixModulesBatchController.get().where(dwrAfterFilter)})
              //            cacheGroupByDwr = cacheGroupByDwr.where(dwrAfterFilter)
              LOG.warn("hiveClient cacheGroupByDwr filter by dwr.filter.after", dwrAfterFilter)
            }

            moduleTracer.trace("dwr after filter")
          }


          //-----------------------------------------------------------------------------------------------------------------
          // Persistence
          //-----------------------------------------------------------------------------------------------------------------

          // Count
          if (isEnableDwiUuid) LOG.warn("cacheUuidRepeats.count()", cacheUuidRepeats.count())
          val dwiCount = offsetDetail.map(_._2).reduce(_+_) //cacheDwi.count()
          var dwrCount = 0L
          if(isEnableDwr) {
            dwrCount = mixModulesBatchController.get().count()
          }
          LOG.warn("cacheDwi.count()", dwiCount)
          if (isEnableDwr && isMaster) LOG.warn("cacheGroupByDwr.count()", dwrCount)

          val isTimeToCommit = (new Date().getTime - lastCommitTime.getTime) > 1000 * commitTimeInterval

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

          //val tid = tidTimeFormat.format(new Date)

          var uuidT, dwrT, dwiT, dwiPhoenixT, kafkaT, dmT: TransactionCookie = null
          //try {

          //---------------------------------- Persist START ----------------------------------
          var pDwi: DataFrame = null
          var pDwr: DataFrame = null
          var ts = Array("repeated", "l_time", "b_date")
          var iPs: Array[Array[HivePartitionPart]] = null
          var rPs: Array[Array[HivePartitionPart]] = null

          if(isEnableDwi) {
            pDwi = cacheDwi
              .selectExpr(
                s"*",
                s"cast(to_date($businessTimeExtractBy) as string)  as b_date"
              )
              .persist()

            iPs = pDwi
              .dropDuplicates(ts)
              .collect()
              .map { x =>
                ts.map { y =>
                  HivePartitionPart(y, x.getAs[String](y))
                }
              }
            moduleTracer.trace("dwi persist")
          }

          ts = Array("l_time", "b_date")
          if(isEnableDwr) {
            pDwr = mixModulesBatchController
              .get()
              .selectExpr((
                dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias ++
                  aggExprsAlias :+
                  s"l_time" :+
                  s"b_date"
                ): _*)
              .persist(StorageLevel.MEMORY_ONLY_SER)

            rPs = pDwr
              .dropDuplicates(ts)
              .collect()
              .map { x =>
                ts.map { y =>
                  HivePartitionPart(y, x.getAs[String](y))
                }
              }
            moduleTracer.trace("dwr persist")
          }
          //        mixTransactionManager.strategyInitBatch(iPs, rPs)
          //---------------------------------- Persist END ----------------------------------

          //-----------------------------------------------------------------------------------------------------------------
          // Save
          //-----------------------------------------------------------------------------------------------------------------
          //        if (isEnableDwiUuid && dwiCount > 0) {
          //          //事务
          //          uuidT = hbaseClient.puts(
          //            parentTid,
          //            dwiUuidStatHbaseTable,
          //            cacheUuidRepeats
          //              .rdd
          //              .map { x =>
          //                UuidStat(
          //                  x.getAs[String]("uuid"),
          //                  x.getAs[Long]("repeats").toInt
          //                )//.setRowkey(Bytes.toBytes(x.getAs[String]("uuid")))
          //              }
          //              .collect()
          //          )
          //          LOG.warn("hbaseClient.puts uuid completed", uuidT)
          //          moduleTracer.trace("hbase puts uuid")
          //        }

          if (isEnableDwr && dwrCount > 0) {

            if(isMaster) {
              dwrT = hiveClient.overwriteUnionSum(
                parentTid,
                dwrTable,
                pDwr,
                aggExprsAlias,
                unionAggExprsAndAlias,
                dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias,
                rPs,
                "l_time",
                "b_date"
              )

              LOG.warn("hiveClient.overwriteUnionSum dwrTable completed", dwrT)
              moduleTracer.trace("hvie dwr save")
            }
          }
          // l_time: The time of the data is saved
          // l_date: The date of the data is saved, Use for partition
          // b_date: The date of the data business time

          //(b_date="2017-06-10" and l_time="2017-06-21_11-00-00")
          // overwrite 1 hour data
          if (isEnableDwi && dwiCount >0) {
            dwiT = hiveClient.into(
              parentTid,
              dwiTable,
              pDwi,
              //            "l_time",
              //            "b_date",
              iPs
            )
            LOG.warn("hiveClient.into dwiTable completed", dwiT)
            moduleTracer.trace("hvie dwi save")
            //          traceBatchUsingTime("hvie dwi save", lastTraceTime, traceBatchUsingTimeLog)
          }

          kafkaT = kafkaClient.setOffset(parentTid, offsetRanges)

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


          if (isEnablePhoenixDm && isMaster && dwrCount >0) {
            dmT = phoenixClient.upsertUnionSum(
              parentTid, /* parentTid +TransactionManager.parentTransactionIdSeparator + "5",*/
              dmPhoenixTable,
              dmHBaseStorableClass,
              mixModulesBatchController.get().selectExpr(
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

          if(dwiPhoenixEnable && dwiCount > 0) {

            val c = Class.forName(dwiPhoenixHBaseStorableClass).asInstanceOf[Class[_<:HBaseStorable]]
            val dwiP = cacheDwi
              .selectExpr(
                s"*",
                //              s"l_time",
                s"cast(to_date($businessTimeExtractBy) as string)  as b_date"
              )
              .toJSON
              .rdd
              .map {x =>
                OM.toBean(x, c)
              }
              .persist(StorageLevel.MEMORY_ONLY_SER)

            moduleTracer.trace("hbase dwi persist")

            LOG.warn("hbaseClient dwi.phoenix putsNonTransaction starting, take(2)", dwiP.take(2))

            if(dwiPhoenixSubtableEnable) {
              hbaseClient.putsNonTransactionMultiSubTable(
                dwiPhoenixTable,
                dwiP
              )
            }else {
              hbaseClient.putsNonTransaction(
                dwiPhoenixTable,
                dwiP
              )
            }

            //收集当前批次数据总量
            MC.push(PushReq(HBASE_WRITECOUNT_BATCH_TOPIC, dwiCount.toString))
//            messageClient.pushMessage(new MessagePushReq(HBASE_WRITECOUNT_BATCH_TOPIC, dwiCount ))

            monitorclient.push(new MonitorMessage("hbase_write_topic", CSTTime.now.time(), dwiCount))

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

          dwiExtendedFieldsHandlerCookies.foreach{x=>
            x._2.foreach{y=>
              cacheTransactionCookies(COOKIE_KIND_DWI_EXT_FIELDS_T, y)
            }
          }
          cacheTransactionCookies(COOKIE_KIND_UUID_T, uuidT)
          cacheTransactionCookies(COOKIE_KIND_DWI_T, dwiT)
          cacheTransactionCookies(COOKIE_KIND_DWR_T, dwrT)
          //        cacheTransactionCookies(COOKIE_KIND_DWI_PHOENIX_T, dwiPhoenixT)
          cacheTransactionCookies(COOKIE_KIND_KAFKA_T, kafkaT)
          cacheTransactionCookies(COOKIE_KIND_DM_T, dmT)

          //------------------------------------------------------------------------------------
          //Commit
          //------------------------------------------------------------------------------------

          if (isEnableDwiUuid && dwiCount > 0) {
            //          hbaseClient.commit(uuidT)
            //          LOG.warn("hbaseClient.commit(t1) completed(put cacheUuidRepeats)", uuidT)
            //          moduleTracer.trace("hbase uuid commit")
          }

          if (isEnableDwr && isMaster && dwrCount > 0) {
            hiveClient.commit(dwrT)
            LOG.warn("hiveClient.commit(t2) completed(overwrite dwr)", dwrT)
            moduleTracer.trace("hive dwr commit")
            //          traceBatchUsingTime("hive dwr commit", lastTraceTime, traceBatchUsingTimeLog)
          }

          if(isEnableDwr && dwrGreenplumEnable && dwrCount > 0) {

            if(!isMaster) {
              throw new ModuleException("Module of include 'dwr.greenplum.enable' must config: master=true")
            }
            greenplumClient.overwrite(
              dwrTable,
              dwrTable,
              dwrT.asInstanceOf[HiveRollbackableTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct,
              "b_date");
            moduleTracer.trace("greenplum dwr refreshed")
            //            traceBatchUsingTime("greenplum dwr refreshed", lastTraceTime, traceBatchUsingTimeLog)
          }

          if (isEnableDwi && dwiCount > 0) {
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
          pushMessageV2(dwrT.asInstanceOf[HiveTransactionCookie], dwiT.asInstanceOf[HiveTransactionCookie])

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
          if(isEnableDwr && dmGreenplumEenable && dwrCount > 0) {

            if(!isMaster) {
              throw new ModuleException("Module of include 'dm.greenplum.enable' must config: master=true")
            }

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

          if (isEnableKafkaDm && isMaster && dwrCount > 0) {
            val k = mixModulesBatchController.get().selectExpr((
              dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias ++
                aggExprsAlias :+
                //s"times" :+
                s"l_time as dwrLoadTime" :+
                s"b_date as dwrBusinessDate"
              ): _*)
              .toJSON
              //.withColumn("l_time", expr(s"'${dwrLoadTimeFormat.format(DateTimeUtil.uniqueSecondes())}'"))
              .collect()

            LOG.warn(s"kafkaClient.sendToKafka $dmKafkaTopic stating, count", k.length )
            //          k.foreach { x =>
            //              kafkaClient.sendToKafka(dmKafkaTopic, x)
            //            }
            kafkaClient.sendToKafka(dmKafkaTopic, k:_*)
            LOG.warn(s"kafkaClient.sendToKafka $dmKafkaTopic completed, take(2)", k.take(2) )
            moduleTracer.trace("dwr to kafka")
            //          traceBatchUsingTime("dwr to kafka", lastTraceTime, traceBatchUsingTimeLog)
          }

          moduleTracer.pauseBatch()
          LOG.warn(s"wait transaction commit starting", s"tid:  $parentTid\nmaster: $isMaster")
          mixTransactionManager.commitTransaction(isMaster, moduleName, {
            //            if (isEnableDwr) {
//            mixTransactionManager.commitTransaction0(isMaster, parentTid, moduleName)
            mixModulesBatchController.completeBatch(isMaster)
            //            }
          })
          LOG.warn(s"wait transaction commit completed")
          moduleTracer.trace("wait master tx commit")
          moduleTracer.continueBatch()

          lastCommitTime = new Date()

          //------------------------------------------------------------------------------------
          //Clean
          //------------------------------------------------------------------------------------
          if(dwi != null) dwi.unpersist()
          cacheDwi = null

          if(saved != null) saved.unpersist()

          if (cacheUuidRepeats != null /*&& mixTransactionManager.needTransactionalAction()*/) {
            cacheUuidRepeats.unpersist()
            cacheUuidRepeats = null
          }
          //uuidT, dwrT, dwiT, kafkaT,
          if (isEnableDwiUuid && dwiCount > 0) {
            //          hbaseClient.clean(popNeedCleanTransactions(COOKIE_KIND_UUID_T, parentTid):_*/*uuidT*/)
            //          moduleTracer.trace("clean hbase uuid")
          }

          if(hiveCleanable != null) {
            hiveCleanable.doActions()
            hiveCleanable = null
          }
          if(hbaseCleanable != null) {
            hbaseCleanable.doActions()
            hbaseCleanable = null
          }
          if(kafkaCleanable != null) {
            kafkaCleanable.doActions()
            kafkaCleanable = null
          }
          if(phoenixCleanable != null) {
            phoenixCleanable.doActions()
            phoenixCleanable = null
          }

          if (isEnableDwr && isMaster && dwrCount > 0) {
            hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWR_T, parentTid):_*/*dwrT*/)
            moduleTracer.trace("clean hive dwr")
            //          traceBatchUsingTime("clean hive dwr", lastTraceTime, traceBatchUsingTimeLog)
          }
          if (isEnableDwi && dwiCount > 0) {
            hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWI_T, parentTid):_* /*dwiT*/)
            moduleTracer.trace("clean hive dwi")
            //          traceBatchUsingTime("clean hive dwi", lastTraceTime, traceBatchUsingTimeLog)
          }
          if(dwiExtendedFieldsHandlerCookies != null) {
            dwiExtendedFieldsHandlerCookies.foreach{ x=>
              x._2.foreach{y=>
                x._1.clean(popNeedCleanTransactions(COOKIE_KIND_DWI_EXT_FIELDS_T, parentTid):_* /*Array(y):_**/)
                moduleTracer.trace("clean dwi extended handler")
                //              traceBatchUsingTime("clean dwi extended handler", lastTraceTime, traceBatchUsingTimeLog)
              }
            }
          }

          kafkaClient.clean(popNeedCleanTransactions(COOKIE_KIND_KAFKA_T, parentTid):_* )
          moduleTracer.trace("clean kafka")
          //        traceBatchUsingTime("clean kafka", lastTraceTime, traceBatchUsingTimeLog)

          if (isEnablePhoenixDm) phoenixClient.clean(popNeedCleanTransactions(COOKIE_KIND_DM_T, parentTid):_*/* dmT*/)

          //Reset
          dwiExtendedFieldsHandlerCookies = Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()

          //      transactionManager.clean(tid)
          //        traceBatchUsingTime("clean", lastTraceTime, traceBatchUsingTimeLog)

          GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

          moduleTracer.trace("set module status")
          //      traceBatchUsingTime("set module status", lastTraceTime, traceBatchUsingTimeLog)

          moduleTracer.endBatch()
          //      batchUsingTime = batchUsingTime + ( new Date().getTime - batchBeginTime)
          mySqlJDBCClientV2.execute(
            s"""
               |  insert into module_running_status(
               |    app_name,
               |    module_name,
               |    batch_using_time,
               |    batch_actual_time,
               |    update_time,
               |    rebrush,
               |    batch_buration,
               |    batch_using_time_trace,
               |    lag
               |  )
               |  values(
               |    "$appName",
               |    "$moduleName",
               |    ${moduleTracer.getBatchUsingTime()},
               |    ${moduleTracer.getBatchActualTime()},
               |    now(),
               |    "${argsConfig.get(ArgsConfig.REBRUSH, "NA").toUpperCase}",
               |    ${(100.0*config.getInt("spark.conf.streaming.batch.buration")/60).asInstanceOf[Int]/100.0},
               |    "${moduleTracer.getTraceResult()}",
               |    $kafkaConsumeLag
               |  )
               |  on duplicate key update
               |    app_name = values(app_name),
               |    batch_using_time = values(batch_using_time),
               |    batch_actual_time = values(batch_actual_time),
               |
               |    update_time = values(update_time),
               |    rebrush = values(rebrush),
               |    batch_buration = values(batch_buration),
               |    batch_using_time_trace = values(batch_using_time_trace),
               |    lag = $kafkaConsumeLag
             """.stripMargin)

          if(isEnableDwi) pDwi.unpersist()
          if(isEnableDwr) pDwr.unpersist()
        }
      }catch {
        case e:Exception=> throw new ModuleException(s"${classOf[MixModule].getSimpleName} '$moduleName' execution failed !! ", e)
      }
    }
  }

  private def upsertGreenplum(dwiT:TransactionCookie, dwrT:TransactionCookie): Unit ={
    dmGreenplumUpsertByHiveViews.map{x=>
      var createViewSQL = hiveClient.sql(s"show create table ${x._1}").first().getAs[String]("createtab_stmt")

      var parts = Array[HivePartitionPart]()
      val dmV = s"${x._1}_4_upsert"
      createViewSQL = createViewSQL.replaceAll("CREATE VIEW", "CREATE OR REPLACE TEMPORARY VIEW").replaceAll(x._1, dmV)

      if(StringUtil.notEmpty(x._2)) {
        val dwiV= s"${x._2}_4_upsert"
        cacheDwi.selectExpr("*", s"cast(to_date($businessTimeExtractBy) as string)  as b_date").createOrReplaceTempView(dwiV)
        createViewSQL = createViewSQL.replaceAll(s"`default`.`${x._2}`", dwiV)
        parts ++= dwiT.asInstanceOf[HiveTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct
      }
      if(StringUtil.notEmpty(x._3)) {
        val dwrV = s"${x._3}_4_upsert"
        mixModulesBatchController.get.createOrReplaceTempView(dwrV)
        createViewSQL = createViewSQL.replaceAll(s"`default`.`${x._3}`", dwrV)
        parts ++= dwrT.asInstanceOf[HiveTransactionCookie].partitions.flatMap{ x=>x}.filter{ x=>"b_date".equals(x.name)}.distinct
      }

      hiveClient.sql(createViewSQL)

      greenplumClient.upsert(x._1, hiveContext.read.table(dmV), x._4, parts, "b_date")

      if(parts.length > 0) moduleTracer.trace(s"gp upsert $x")
    }
  }
  private def overwrideGreenplum (): Unit ={
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

  //<cookieKind, <transactionParentId, transactionCookies>>
  private def cacheTransactionCookies(cookieKind: String, transactionCookie: TransactionCookie): Unit ={

    if(transactionCookie.isInstanceOf[HiveRollbackableTransactionCookie]
      || transactionCookie.isInstanceOf[KafkaRollbackableTransactionCookie]
      || transactionCookie.isInstanceOf[HBaseTransactionCookie]
    ) {

      var pr = batchsTransactionCookiesCache.get(cookieKind)
      if(pr == null) {
        pr = new util.ArrayList[TransactionCookie]()
        batchsTransactionCookiesCache.put(cookieKind, pr)
      }
      pr.add(transactionCookie)
    }
  }

  private val EMPTY_TRANSACTION_COOKIES = Array[TransactionCookie]()

  //找到并移除
  private def popNeedCleanTransactions(cookieKind: String, excludeCurrTransactionParentId: String): Array[TransactionCookie] ={
    var result = EMPTY_TRANSACTION_COOKIES
    var pr = batchsTransactionCookiesCache.get(cookieKind)
    var isTran = mixTransactionManager.needTransactionalAction()
    if(pr != null && isTran){
      var needCleans = pr.filter(!_.parentId.equals(excludeCurrTransactionParentId))
      pr.removeAll(needCleans)
      result = needCleans.toArray
    }
    result
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
  //      c.add(Calendar.DATE, 1)
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

  private def pushMessageV2 (dwrT:HiveTransactionCookie, dwiT:HiveTransactionCookie): Unit ={

    var topic:String = null

    if(dwiT != null && dwiT.partitions != null && dwiT.partitions.nonEmpty) {
      topic = dwiT.targetTable

      val key = OM.toJOSN(dwiT.partitions.map{x=> x.sortBy{y=> y.name + y.value}}.sortBy{x=> OM.toJOSN(x)})
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient pushMessage completed", s"topic: $topic, \nkey: $key")

      topic = moduleName
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient pushMessage completed", s"topic: $topic, \nkey: $key")

    }else {
      LOG.warn(s"MessageClient dwi no update hive partitions to push",  s"topic: $topic")
    }

    if(dwrT != null && dwrT.partitions != null && dwrT.partitions.nonEmpty) {
      val topic = dwrT.targetTable
      var key = OM.toJOSN(dwrT.partitions.map{x=> x.sortBy{y=> y.name + y.value}}.sortBy{x=> OM.toJOSN(x)})
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient pushMessage completed", s"topic: $topic, \nkey: $key")
    }else {
      LOG.warn(s"MessageClient dwr no update hive partitions to push",  s"topic: $topic")
    }

  }

  @Deprecated
  private def pushMessage (dwrT:HiveTransactionCookie, dwiT:HiveTransactionCookie): Unit ={
    var ps:Array[Array[HivePartitionPart]] = null

    if(dwiT != null ) {
      ps = dwiT.partitions
    }

    if(dwrT != null && ps == null) {
      ps = dwrT.partitions
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

      //待优化，增大事务周期
      if(dwrT != null) {
        messageClient.pushMessage(new MessagePushReq(dwrT.targetTable, key, uniqueKey, data))
        LOG.warn(s"MessageClient pushMessage completed", s"topic: $moduleName, \nkey: $key, \nuniqueKey: $uniqueKey, \ndata: $data")
      }

      if(dwiT != null) {
        messageClient.pushMessage(new MessagePushReq(dwiT.targetTable, key, uniqueKey, data))
        LOG.warn(s"MessageClient pushMessage completed", s"topic: $moduleName, \nkey: $key, \nuniqueKey: $uniqueKey, \ndata: $data")
      }
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
