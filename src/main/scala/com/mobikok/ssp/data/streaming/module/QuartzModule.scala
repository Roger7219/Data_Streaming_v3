package com.mobikok.ssp.data.streaming.module

import java.text.DecimalFormat
import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.dm.offline.{ClickHouseQueryByBDateHandler, ClickHouseQueryByBTimeHandler}
import com.mobikok.ssp.data.streaming.module.support._
import com.mobikok.ssp.data.streaming.module.support.uuid.{DefaultUuidFilter, UuidFilter}
import com.mobikok.ssp.data.streaming.util.{MC, YarnAPPManagerUtil, _}
import com.typesafe.config.Config
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.streaming.StreamingContext
import org.quartz.Scheduler

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Administrator on 2017/6/8.
  */
class QuartzModule(config: Config,
                   argsConfig: ArgsConfig,
                   concurrentGroup: String,
                   mixModulesBatchController: MixModulesBatchController,
                   moduleName: String,
                   runnableModuleNames: Array[String],
                   /* dwiStructType: StructType,*/
                   ssc: StreamingContext) extends Module {



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

  //<cookieKind, <transactionParentId, transactionCookies>>
  var batchsTransactionCookiesCache = new util.HashMap[String, util.ArrayList[TransactionCookie]]()//mutable.Map[String, ListBuffer[TransactionCookie]]()

  val hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
  val sqlContext = SQLContextGenerater.generate(ssc.sparkContext)
  var kylinClient: KylinClientV2 = null
  try {
    kylinClient = new KylinClientV2(config.getString("kylin.client.url"))
  }catch {
    case e:Exception=> LOG.warn("KylinClient init fail !!", e.getMessage)
  }

  var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    moduleName, config.getString(s"rdb.url"), config.getString(s"rdb.user"), config.getString(s"rdb.password")
  )

  var moduleTracer: ModuleTracer = new ModuleTracer(moduleName, config, mixModulesBatchController)

  var greenplumClient = null.asInstanceOf[GreenplumClient]
  try {
    greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  }catch {case e:NoClassDefFoundError => LOG.warn("GreenplumClient no class found", e.getMessage)}

  var bigQueryClient = null.asInstanceOf[BigQueryClient]
  try{
    bigQueryClient = new BigQueryClient(moduleName, config, ssc, messageClient, hiveContext)
  }catch {case e:Throwable => LOG.warn("BigQueryClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")}

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

  val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS")

  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(config.getString(s"modules.$moduleName.dm.load.time.format.by"))
  }catch { case _: Exception =>}

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

  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwiBTimeFormat = config.getString(s"modules.$moduleName.dwi.business.time.format.by")
  }catch {case e: Throwable=>}
  try {
    dwrBTimeFormat = config.getString(s"modules.$moduleName.dwr.business.time.format.by")
  }catch {case e: Throwable=>}

  var businessTimeExtractBy: String = null
  try {
    businessTimeExtractBy = config.getString(s"modules.$moduleName.business.time.extract.by")
  }catch {case e:Throwable=>
    //兼容历史代码
    try{
      businessTimeExtractBy = config.getString(s"modules.$moduleName.business.date.extract.by")
    }catch {case e:Throwable=>
      businessTimeExtractBy = config.getString(s"modules.$moduleName.b_time.input")
    }
  }

  val topics = config.getConfigList(s"modules.$moduleName.kafka.consumer.partitions").map { x => x.getString("topic") }.toArray[String]

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

  var cron_exp = "0 0/5 * * * ?"
  try {
    cron_exp = config.getString(s"modules.$moduleName.dm.handler.cron_exp")
  } catch {
    case _: Exception =>
  }

  if (isEnableHandlerDm) {
    dmHandlers = new util.ArrayList[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]()
    config.getConfigList(s"modules.$moduleName.dm.handler.setting").foreach { x =>
      var h = Class.forName(x.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]
      h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient,kylinClient, hbaseClient, hiveContext, argsConfig, x)
      if (h.isInstanceOf[ClickHouseQueryByBTimeHandler] || h.isInstanceOf[ClickHouseQueryByBDateHandler]) {
        h.setClickHouseClient(new ClickHouseClient(moduleName, config, ssc, messageClient, mixTransactionManager, hiveContext, moduleTracer))
      }
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
    dwiUuidFields =
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

  //  @volatile var cacheGroupByDwr: DataFrame = null
  //对于没有重复的数据hbase repeats:1, hive repeats :0
  var cacheUuidRepeats: DataFrame = null
  @volatile var cacheDwi: DataFrame = null

  var uuidFilter: UuidFilter = null
  if(config.hasPath(s"modules.$moduleName.dwr.uuid.filter")) {
    var f = config.getString(s"modules.$moduleName.dwr.uuid.filter")
    uuidFilter = Class.forName(f).newInstance().asInstanceOf[UuidFilter]
  }else {
    uuidFilter = new DefaultUuidFilter()
  }
  uuidFilter.init(moduleName, config, hiveContext, moduleTracer, dwiUuidFieldsAlias, businessTimeExtractBy, dwiTable)

  var isFastPollingEnable = false
  if(config.hasPath(s"modules.$moduleName.fast.polling.enable")) {
    isFastPollingEnable = config.getBoolean(s"modules.$moduleName.fast.polling.enable")
  }

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

        h.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, hc, config, col/*x.getString("expr")*/, as.toArray( Array[String]() ))
        (as, expr(col), h)
      }.toList
    }

  }

  val duration = if(cron_exp.split(" ")(1).contains("/")){
    cron_exp.split(" ")(1).split("\\/")(1).toInt * 100.0 /100.0
  }else{
    0.0
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

      cloneTables(moduleName)

      import org.quartz.CronScheduleBuilder
      import org.quartz.JobBuilder
      import org.quartz.JobDetail
      import org.quartz.SchedulerFactory
      import org.quartz.Trigger
      import org.quartz.TriggerBuilder
      import org.quartz.impl.StdSchedulerFactory

      val schedulerFactory: SchedulerFactory = new StdSchedulerFactory
      val scheduler: Scheduler  = schedulerFactory.getScheduler
      val jobDetail: JobDetail = JobBuilder.newJob(classOf[QuartzJob]).build

      jobDetail.getJobDataMap.put(QuartzJob.QUARTZ_MODULE, this)
//      jobDetail.getJobDataMap.put("NumExecutions",4)
      val trigger: Trigger = TriggerBuilder
        .newTrigger
        .withIdentity(s"${moduleName}_cronJob", "cronGroup")
        .withSchedule(CronScheduleBuilder
          .cronSchedule(cron_exp).withMisfireHandlingInstructionIgnoreMisfires()//withMisfireHandlingInstructionFireAndProceed()
        )
        .startNow
        .build

      scheduler.scheduleJob(jobDetail, trigger)
      if (!scheduler.isShutdown)
        scheduler.start()

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
         |    $duration,
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


      LOG.warn(s"${getClass.getSimpleName} init completed", moduleName)
    } catch {
      case e: Exception =>
        throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' init fail, Transactionals rollback exception", e)
    }
  }

  //clone 模块创建相应表
  def cloneTables(moduleName: String): Unit = {

    if(argsConfig.has(ArgsConfig.CLONE) && "true".equals(argsConfig.get(ArgsConfig.CLONE))){

      val clonePrefix = "clone_"

      val dwiTPath = s"modules.$moduleName.dwi.table"
      val dwiEnable = s"modules.$moduleName.dwi.enable"
      val dwrTPath = s"modules.$moduleName.dwr.table"
      val dwrEnable = s"modules.$moduleName.dwr.enable"
      val hbTPath = s"modules.$moduleName.dwi.phoenix.table"
      val hbEnable= s"modules.$moduleName.dwi.phoenix.enable"
      val master= s"modules.$moduleName.master"

      if(config.hasPath(dwiTPath) && config.getBoolean(dwiEnable)){
        val dwiT = config.getString(dwiTPath)
        hiveContext.sql(s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwi Table", s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")

      }

      if(config.hasPath(dwrTPath) && config.getBoolean(dwrEnable) && config.hasPath(master) && config.getBoolean(master)){
        val dwrT = config.getString(dwrTPath)
        hiveContext.sql(s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwr Table", s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")

      }

      if(config.hasPath(hbTPath) && config.getBoolean(hbEnable)){
        val hbT = config.getString(hbTPath)
        hbaseClient.createTableIfNotExists(hbT, hbT.split("_")(1))
        LOG.warn("cloneTables hbase Table", s"$hbT--${hbT.split("_")(1)}")

      }

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
    try{

      moduleTracer.startBatch
      GlobalAppRunningStatusV2.waitRunAndSetRunningStatus(concurrentGroup, moduleName)
      moduleTracer.trace("wait last batch")

      // dm handler...
      dmHandlers.foreach { x =>
        LOG.warn(s"dm handler(${x.getClass.getSimpleName}) starting")
        x.handle()
        LOG.warn(s"dm handler(${x.getClass.getSimpleName}) completed")
      }

      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)
      moduleTracer.trace("batch done")

      moduleTracer.endBatch()

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
           |    $duration,
           |    "${moduleTracer.getHistoryBatchesTraceResult()}",
           |    0
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
           |    lag = 0
             """.stripMargin)


      var killSelfApp = false
      MC.pull("kill_self_cer", Array(s"kill_self_$appName"), { x=>
        if(x.nonEmpty) {
          if(isMaster) {
            // 尽量等待所有module完成
            Thread.sleep(10*1000)
            killSelfApp = true
          }else {
            //等待master module kill自身app
            while(true) {
              Thread.sleep(2000)
            }
          }
          true
        }else{
          false
        }
      })

      if(killSelfApp) {
        LOG.warn(s"Kill self yarn app via user operation !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName)
        YarnAPPManagerUtil.killApps(appName)
      }


    }catch {
      case e:Exception=> throw new ModuleException(s"${classOf[QuartzModule].getSimpleName} '$moduleName' execution failed !! ", e)
    }
  }


  def stop (): Unit = {

  }


}