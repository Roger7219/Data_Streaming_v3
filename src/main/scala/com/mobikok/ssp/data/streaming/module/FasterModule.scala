package com.mobikok.ssp.data.streaming.module

import java.text.DecimalFormat
import java.util
import java.util.Date

import com.google.protobuf.Message
import com.mobikok.message.MessagePushReq
import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, LatestOffsetRecord, OffsetRange}
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.dm.offline.{ClickHouseQueryByBDateHandler, ClickHouseQueryByBTimeHandler, ClickHouseQueryMonthHandler, Handler}
import com.mobikok.ssp.data.streaming.handler.dwi.core.UUIDFilterDwiHandler
import com.mobikok.ssp.data.streaming.handler.dwr.core.UUIDFilterDwrHandler
import com.mobikok.ssp.data.streaming.module.support._
import com.mobikok.ssp.data.streaming.module.support.uuid.{DefaultUuidFilter, UuidFilter}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by Administrator on 2017/6/8.
  */
class FasterModule(config: Config,
                   argsConfig: ArgsConfig,
                   concurrentGroup: String,
                   mixModulesBatchController: MixModulesBatchController,
                   moduleName: String,
                   runnableModuleNames: Array[String],
                   /* dwiStructType: StructType,*/
                   ssc: StreamingContext) extends Module {

  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  //----------------------  Constants And Fields  -------------------------
  val appName = ssc.sparkContext.getConf.get("spark.app.name")
  val ksc = Class.forName(config.getString(s"modules.$moduleName.dwi.kafka.schema"))
  val dwiStructType = ksc.getMethod("structType").invoke(ksc.newInstance()).asInstanceOf[StructType]
  var moduleConfig = config.getConfig(s"modules.$moduleName")
  val dwiUuidFieldsSeparator = "^"

  //moduleName, transactionCookies

  val COOKIE_KIND_UUID_T = "uuidT"
  val COOKIE_KIND_DWI_EXT_FIELDS_T = "dwiExtFieldsT"
  val COOKIE_KIND_DWI_T = "dwiT"
  val COOKIE_KIND_DWR_T = "dwrT"
  val COOKIE_KIND_DWR_ACC_DAY_T = "dwrAccDayT"
  val COOKIE_KIND_DWR_ACC_MONTH_T = "dwrAccMonthT"
  val COOKIE_KIND_DWR_MYSQL_DAY_T = "dwrMySQLDayT"
  val COOKIE_KIND_DWR_MYSQL_MONTH_T = "dwrMySQLMonthT"
  val COOKIE_KIND_DWR_CLICKHOUSE_T = "dwrClickHouseT"
  val COOKIE_KIND_DWR_CLICKHOUSE_DAY_T = "dwrClickHouseDayT"
  //  val COOKIE_KIND_DWI_PHOENIX_T = "dwiPhoenixT"
  val COOKIE_KIND_KAFKA_T = "kafkaT"
  val COOKIE_KIND_DM_T = "dmT"
  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

  //<cookieKind, <transactionParentId, transactionCookies>>
  var batchsTransactionCookiesCache = new util.HashMap[String, util.ArrayList[TransactionCookie]]() //mutable.Map[String, ListBuffer[TransactionCookie]]()

  val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS")

  val shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  var aggExprs: List[Column] = _
  try {
    aggExprs = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {
    case e: Exception =>
  }

  var aggExprsAlias: List[String] = _
  try {
    aggExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
  } catch {
    case e: Exception =>
  }

  var unionAggExprsAndAlias: List[Column] = _
  try {
    unionAggExprsAndAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList
  } catch {
    case e: Exception =>
  }

  var businessTimeExtractBy: String = _
  try {
    businessTimeExtractBy = config.getString(s"modules.$moduleName.business.time.extract.by")
  } catch {
    case e: Throwable =>
      //兼容历史代码
      try{
        businessTimeExtractBy = config.getString(s"modules.$moduleName.business.date.extract.by")
      }catch {case e:Throwable=>
        businessTimeExtractBy = config.getString(s"modules.$moduleName.b_time.input")
      }
  }

  var isFastPollingEnable = false
  if (config.hasPath(s"modules.$moduleName.fast.polling.enable")) {
    isFastPollingEnable = config.getBoolean(s"modules.$moduleName.fast.polling.enable")
  }

  val monitorclient = new MonitorClient(messageClient)
  val topics = config.getConfigList(s"modules.$moduleName.kafka.consumer.partitoins").map { x => x.getString("topic") }.toArray[String]

  var isExcludeOfflineRebrushPart = false
  if (ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))) {
    isExcludeOfflineRebrushPart = true
  }

  var kafkaProtoEnable = false
  var kafkaProtoClass: Class[_] = _
  try {
    kafkaProtoEnable = config.getBoolean(s"modules.$moduleName.dwi.kafka.proto.enable")
    kafkaProtoClass = Class.forName(config.getString(s"modules.$moduleName.dwi.kafka.proto.class"))
  } catch {
    case _: Exception =>
  }

  var needDwrAccDay = false
  try {
    needDwrAccDay = config.getBoolean(s"modules.$moduleName.dwr.acc.day.enable")
  } catch {
    case _: Exception =>
  }

  var needDwrAccMonth = false
  try {
    needDwrAccMonth = config.getBoolean(s"modules.$moduleName.dwr.acc.month.enable")
  } catch {
    case _: Exception =>
  }

  var needH2Persistence = false
  try {
    needH2Persistence = config.getBoolean(s"modules.$moduleName.h2.dwr.enable")
  } catch {
    case _: Exception =>
  }

  var isMaster = mixModulesBatchController.isMaster(moduleName)

  // h2 数据库参数
  val h2Driver = "org.h2.Driver"
  var h2JDBCConnectionUrl = "jdbc:h2:tcp://node14:10010/mem:campaign;DB_CLOSE_DELAY=-1"
  runInTry{h2JDBCConnectionUrl = config.getString(s"modules.$moduleName.h2.url")}

  var h2PersistFields: Array[(String, String)] = _
  runInTry{
    h2PersistFields = config.getConfigList(s"modules.$moduleName.h2.fields").map{ x =>
      (x.getString("expr"), x.getString("as"))
    }.toArray
  }

  var h2PersistAggFields: Array[(String, String)] = _
  runInTry{
    h2PersistAggFields = config.getConfigList(s"modules.$moduleName.h2.agg").map{ x =>
      (x.getString("expr"), x.getString("as"))
    }.toArray
  }

  var h2TableName = "CampaignSearch"
  runInTry{h2TableName = config.getString(s"modules.$moduleName.h2.table.name")}

  // clickhouse缓存批次数据
  // 小时
  var clickhousePersis = false
  runInTry{ clickhousePersis = config.getBoolean(s"modules.$moduleName.dwr.clickhouse.hour.enable") }
  var clickhousePeresisTable: String =_
//  var clickhousePeresisFields: Array[String] = _
  if (clickhousePersis) {
    clickhousePeresisTable = config.getString(s"modules.$moduleName.dwr.clickhouse.hour.table.name")
//    clickhousePeresisFields = config.getStringList(s"modules.$moduleName.dwr.clickhouse.hour.fields").asScala.toArray
  }
  // 天
  var clickhousePersisDay = false
  runInTry{clickhousePersisDay = config.getBoolean(s"modules.$moduleName.dwr.clickhouse.day.enable")}
  var clickhousePeresisTableDay: String =_
//  var clickhousePeresisFieldsDay: Array[String] = _
  if (clickhousePersisDay) {
    clickhousePeresisTableDay = config.getString(s"modules.$moduleName.dwr.clickhouse.day.table.name")
//    clickhousePeresisFieldsDay = config.getStringList(s"modules.$moduleName.dwr.clickhouse.day.fields").asScala.toArray
  }
  // 月
  var clickhousePersisMonth = false
  runInTry{clickhousePersisMonth = config.getBoolean(s"modules.$moduleName.dwr.clickhouse.month.enable")}
  var clickhousePeresisTableMonth: String =_
//  var clickhousePeresisFieldsMonth: Array[String] = _
  if (clickhousePersisMonth) {
    clickhousePeresisTableMonth = config.getString(s"modules.$moduleName.dwr.clickhouse.month.table.name")
//    clickhousePeresisFieldsMonth = config.getStringList(s"modules.$moduleName.dwr.clickhouse.month.fields").asScala.toArray
  }
  //-------------------------  Constants And Fields End  -------------------------


  //-------------------------  Tools  -------------------------
  //Value Type: String or Array[Byte]
  var stream: InputDStream[ConsumerRecord[String, Object]] = _
  var moduleTracer: ModuleTracer = new ModuleTracer(moduleName, config, mixModulesBatchController)
  //  val transactionManager: TransactionManager = new TransactionManager(config)
  val mixTransactionManager = mixModulesBatchController.getMixTransactionManager()

  //-------------------------  Tools End  -------------------------


  //-------------------------  Clients  -------------------------
  val hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
  val sqlContext = SQLContextGenerater.generate(ssc.sparkContext)
  var kylinClient: KylinClientV2 = _
  try {
    kylinClient = new KylinClientV2(config.getString("kylin.client.url"))
  } catch {
    case e: Exception => LOG.warn("KylinClient init fail !!", e.getMessage)
  }

  var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    moduleName, config.getString(s"rdb.url"), config.getString(s"rdb.user"), config.getString(s"rdb.password")
  )

  var bigQueryClient = null.asInstanceOf[BigQueryClient]
  try {
    bigQueryClient = new BigQueryClient(moduleName, config, ssc, messageClient, hiveContext)
  } catch {
    case e: Throwable => LOG.warn("BigQueryClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")
  }

  val rDBConfig = new RDBConfig(mySqlJDBCClientV2)

  val messageClient = new MessageClient(moduleName, config.getString("message.client.url"))
  var hbaseClient: HBaseMultiSubTableClient = _ //new HBaseClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
  //  if(dwiPhoenixEnable) {
  hbaseClient = new HBaseMultiSubTableClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
  //  }
  val hiveClient = new HiveClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, config, mixTransactionManager)
  val phoenixClient = new PhoenixClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
  //  val greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val greenplumClient = null.asInstanceOf[GreenplumClient]
  var h2JDBCClient = null.asInstanceOf[H2JDBCClient]
  try {
    h2JDBCClient = new H2JDBCClient(h2JDBCConnectionUrl, "", "")
  } catch {
    case e: Exception => LOG.warn(s"Init h2 client failed, skip it, ${e.getMessage}")
  }

  val mysqlClient = new MySQLClient(moduleName, ssc.sparkContext, config, messageClient, mixTransactionManager, moduleTracer)
  var clickHouseClient = null.asInstanceOf[ClickHouseClient]
  try {
    clickHouseClient = new ClickHouseClient(moduleName, config, ssc, messageClient, mixTransactionManager, hiveContext, moduleTracer)
  } catch {case e: Exception => LOG.warn(s"Init clickhouse client failed, skip it, ${e.getMessage}")}
  //-------------------------  Clients End  -------------------------


  //-------------------------  Dwi about  -------------------------
  var dwiPhoenixEnable = false
  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable = null.asInstanceOf[String]
  var dwiPhoenixHBaseStorableClass = null.asInstanceOf[String]
  try {
    dwiPhoenixEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.enable")
  } catch {
    case e: Exception =>
  }
  if (dwiPhoenixEnable) {
    dwiPhoenixTable = config.getString(s"modules.$moduleName.dwi.phoenix.table")
    dwiPhoenixHBaseStorableClass = config.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
  }
  if (config.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")) {
    dwiPhoenixSubtableEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
  }

  var dwiHandlerCookies: Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])] = _ // Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()
  var dwiHandlers: List[(java.util.List[String], Column, com.mobikok.ssp.data.streaming.handler.dwi.Handler)] = _
  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwiBTimeFormat = config.getString(s"modules.$moduleName.dwi.business.time.format.by")
  } catch {
    case e: Throwable =>
  }

  var isEnableDwi = false
  var dwiTable: String = _
  try {
    isEnableDwi = config.getBoolean(s"modules.$moduleName.dwi.enable")
  } catch {
    case e: Exception =>
  }
  try {
    dwiTable = config.getString(s"modules.$moduleName.dwi.table")
  } catch {
    case e: Throwable =>
      if (isEnableDwi) throw e
  }

  var isEnableDwiUuid = false
  var dwiUuidStatHbaseTable: String = _
  var dwiUuidFieldsAlias: String = "rowkey"
  var dwiUuidFields: Array[String] = _
  try {
    isEnableDwiUuid = config.getBoolean(s"modules.$moduleName.dwi.uuid.enable")
  } catch {
    case e: Exception =>
  }
  if (isEnableDwiUuid) {
    dwiUuidFields = //Array(Array("appId", "jarId", "imei"), Array("b_date","appId", "jarId", "imei")) //
      config.getStringList(s"modules.$moduleName.dwi.uuid.fields").toArray[String](new Array[String](0))
    try {
      dwiUuidFieldsAlias = config.getString(s"modules.$moduleName.dwi.uuid.alias")
    } catch {
      case e: Exception =>
    }

    dwiUuidStatHbaseTable = dwiTable + "_uuid"
    try {
      dwiUuidStatHbaseTable = config.getString(s"modules.$moduleName.dwi.uuid.stat.hbase.table")
    } catch {
      case e: Exception =>
    }
  }

  var uuidFilter: UuidFilter = _
  if (config.hasPath(s"modules.$moduleName.dwr.uuid.filter")) {
    var f = config.getString(s"modules.$moduleName.dwr.uuid.filter")
    uuidFilter = Class.forName(f).newInstance().asInstanceOf[UuidFilter]
  } else {
    uuidFilter = new DefaultUuidFilter()
  }
  uuidFilter.init(moduleName, config, hiveContext, moduleTracer, dwiUuidFieldsAlias, businessTimeExtractBy, dwiTable)
  //-------------------------  Dwi End  -------------------------


  //-------------------------  Dwr about  -------------------------
  var dwrGroupByExprs: List[Column] = _
  try {
    dwrGroupByExprs = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => expr(x.getString("overwrite")).as(x.getString("as"))
//      (x.getString("expr"), x.getString("as"), x.getString("overwrite"))
    }.toList
  } catch {
    case e: Exception =>
  }

  var dwrGroupByOverwrite = new util.HashMap[String, (String, String)]()
  try {
    config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").foreach { x =>
//      x => expr(x.getString("groupby"))
      dwrGroupByOverwrite.put(x.getString("overwrite"), (x.getString("expr"), x.getString("groupby")))
    }
  } catch {
    case e: Exception =>
  }

  var dwrGroupByExprsAlias: Array[String] = _
  try {
    dwrGroupByExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray[String]
  } catch {
    case e: Exception =>
  }

  var dwrGroupByExprsAliasCol: List[Column] = _
  try {
    dwrGroupByExprsAliasCol = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => col(x.getString("as"))
    }.toList
  } catch {
    case e: Exception =>
  }

  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwrBTimeFormat = config.getString(s"modules.$moduleName.dwr.business.time.format.by")
  } catch {
    case e: Throwable =>
  }

  var dwrIncludeRepeated = true
  if (config.hasPath(s"modules.$moduleName.dwr.include.repeated")) {
    dwrIncludeRepeated = config.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  } else if (isEnableDwiUuid) {
    dwrIncludeRepeated = false
  }

  var dwrHandlers: List[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = _
  if (!dwrIncludeRepeated && isMaster) {
    dwrHandlers = List(new UUIDFilterDwrHandler(uuidFilter))
  } else {
    dwrHandlers = List()
  }

  try {
    dwrHandlers = dwrHandlers ++ config.getConfigList(s"modules.$moduleName.dwr.handler").map { x =>
      val hc = x.getConfig("handler")
      var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
      h.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, x.getString("expr"), x.getString("as"))
      h
    }.toList
  } catch {
    case e: Exception =>
  }

  var isEnableDwr = false

  var dwrTable: String = _
  try {
    isEnableDwr = config.getBoolean(s"modules.$moduleName.dwr.enable")
  } catch {
    case e: Exception =>
  }
  if (isEnableDwr) {
    dwrTable = config.getString(s"modules.$moduleName.dwr.table")
  }
  //-------------------------  Dwr End  -------------------------

  //-------------------------  Dm about  -------------------------
  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(config.getString(s"modules.$moduleName.dm.load.time.format.by"))
  } catch {
    case _: Exception =>
  }

  var isEnableKafkaDm = false
  var dmKafkaTopic: String = _
  try {
    isEnableKafkaDm = config.getBoolean(s"modules.$moduleName.dm.kafka.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableKafkaDm) {
    dmKafkaTopic = config.getString(s"modules.$moduleName.dm.kafka.topic")
  }

  var isEnablePhoenixDm = false
  var dmPhoenixTable: String = _
  var dmHBaseStorableClass: Class[_ <: HBaseStorable] = _
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
  var dmHandlers: util.List[Handler] = _
  try {
    isEnableHandlerDm = config.getBoolean(s"modules.$moduleName.dm.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableHandlerDm) {
    try {
      dmHandlers = new util.ArrayList[Handler]()
      config.getConfigList(s"modules.$moduleName.dm.handler.setting").foreach { setting =>
        var h = Class.forName(setting.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]
        h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClient, hbaseClient, hiveContext, setting)
        if (h.isInstanceOf[ClickHouseQueryByBTimeHandler] || h.isInstanceOf[ClickHouseQueryByBDateHandler] || h.isInstanceOf[ClickHouseQueryMonthHandler]) {
          h.setClickHouseClient(clickHouseClient)
        }
        dmHandlers.add(h)
      }
    } catch {
      case _: Exception =>
    }
  }
  //-------------------------  Dm End  -------------------------


  def initDwiHandlers(): Unit = {
    val uuidFilterHandler = new UUIDFilterDwiHandler(uuidFilter, businessTimeExtractBy, isEnableDwiUuid, dwiBTimeFormat, argsConfig)
    uuidFilterHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, config, "null", Array[String]())

    if (config.hasPath(s"modules.$moduleName.dwi.handler")) {
      dwiHandlers = config.getConfigList(s"modules.$moduleName.dwi.handler").map { x =>
        val hc = x.getConfig("handler")
        var h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
        var col = "null"
        var as: java.util.List[String] = new util.ArrayList[String]()

        try {
          if (StringUtil.notEmpty(x.getString("expr"))) col = x.getString("expr")
        } catch {
          case ex: Throwable =>
        }

        try {
          as = x.getStringList("as")
        } catch {
          case ex: Throwable =>
        }

        h.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, hc, config, col /*x.getString("expr")*/ , as.toArray(Array[String]()))
        LOG.warn("init dwi handler", h.getClass.getName)
        (as, expr(col), h)
      }.toList
    }
    dwiHandlers = (new util.ArrayList[String](), expr("null"), uuidFilterHandler) :: (if (dwiHandlers == null) Nil else dwiHandlers)
  }

  @volatile var hiveCleanable, mysqlCleanable, clickHouseCleanable, hbaseCleanable, kafkaCleanable, phoenixCleanable: Cleanable = _

  override def init(): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} init started", moduleName)
      //      AS3.main3(sqlContext)
      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      hiveClient.init()
      if (hbaseClient != null) hbaseClient.init()
      kafkaClient.init()
      phoenixClient.init()
      mysqlClient.init()
      clickHouseClient.init()

      initDwiHandlers()

      hiveCleanable = hiveClient.rollback()
      mysqlCleanable = mysqlClient.rollback()
      if (hbaseClient != null) hbaseCleanable = hbaseClient.rollback()
      kafkaCleanable = kafkaClient.rollback()
      phoenixCleanable = phoenixClient.rollback()
      clickHouseCleanable = clickHouseClient.rollback()

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
           |    ${(100.0 * config.getInt("spark.conf.streaming.batch.buration") / 60).asInstanceOf[Int] / 100.0},
           |    -1,
           |    -1
           |  )
           |  on duplicate key update
           |    app_name = values(app_name),
           |    update_time = values(update_time),
           |    rebrush = values(rebrush),
           |    batch_buration = values(batch_buration),
           |    batch_using_time = -1,
           |    batch_actual_time = -1
                 """.stripMargin)

      initHeartbeat()
      //更新状态
      mixTransactionManager.clean(moduleName)

      LOG.warn(s"${getClass.getSimpleName} init completed", moduleName)
    } catch {
      case e: Exception =>
        throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' init fail, Transactionals rollback exception", e)
    }
  }

  def initHeartbeat(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        val last = ""
        while (true) {
          LOG.warn("Module heartbeat", moduleTracer.getHistoryBatchesTraceResult())

          Thread.sleep(1000 * 60 * 3L)
        }
      }
    }).start()
  }

  //clone 模块创建相应表
  def cloneTables(moduleName: String): Unit = {

    if (argsConfig.has(ArgsConfig.CLONE) && "true".equals(argsConfig.get(ArgsConfig.CLONE))) {

      val clonePrefix = "clone_"

      val dwiTPath = s"modules.$moduleName.dwi.table"
      val dwiEnable = s"modules.$moduleName.dwi.enable"
      val dwrTPath = s"modules.$moduleName.dwr.table"
      val dwrEnable = s"modules.$moduleName.dwr.enable"
      val hbTPath = s"modules.$moduleName.dwi.phoenix.table"
      val hbEnable = s"modules.$moduleName.dwi.phoenix.enable"
      val master = s"modules.$moduleName.master"

      if (config.hasPath(dwiTPath) && config.getBoolean(dwiEnable)) {
        val dwiT = config.getString(dwiTPath)
        hiveContext.sql(s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwi Table", s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")

      }

      if (config.hasPath(dwrTPath) && config.getBoolean(dwrEnable) && config.hasPath(master) && config.getBoolean(master)) {
        val dwrT = config.getString(dwrTPath)
        hiveContext.sql(s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwr Table", s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")

      }

      if (config.hasPath(hbTPath) && config.getBoolean(hbEnable)) {
        val hbT = config.getString(hbTPath)
        hbaseClient.createTableIfNotExists(hbT, hbT.split("_")(1))
        LOG.warn("cloneTables hbase Table", s"$hbT--${hbT.split("_")(1)}")

      }

    }

  }

  def start(): Unit = {

  }

  val traceBatchUsingTimeFormat = new DecimalFormat("#####0.00")

  @volatile var moduleReadingKafkaMarks = mutable.Map[String, Boolean]()

  def handler(): Unit = {

    stream = kafkaClient.createDirectStream(config, ssc, kafkaClient.getCommitedOffset(topics: _*), moduleName)

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
            if (v.isEmpty) {
              (x._1, 0.asInstanceOf[Long], 0.asInstanceOf[Long])
            } else {
              (x._1, v(0).untilOffset - v(0).fromOffset, x._2 - v(0).untilOffset)
            }
          }

        var kafkaConsumeLag = 0.asInstanceOf[Long]
        offsetDetail.foreach { x =>
          kafkaConsumeLag = kafkaConsumeLag + x._3
        }

        moduleTracer.trace(s"offset detail cnt: ${offsetDetail.map(_._2).sum} lag: ${offsetDetail.map(_._3).sum}\n" + offsetDetail.map { x => s"        ${x._1.topic}, ${x._1.partition} -> cnt: ${x._2} lag: ${x._3}" }.mkString("\n"))

        moduleTracer.trace("wait last batch", {
          while (moduleReadingKafkaMarks.getOrElse(moduleName, false)) {
            Thread.sleep(1000)
          }
          moduleReadingKafkaMarks.put(moduleName, true)
        })

        LOG.warn("Kafka Offset Range", offsetRanges)

        //For Task serializable
        val _kafkaProtoClass = kafkaProtoClass

        var jsonSource: RDD[String] = null
        if (kafkaProtoEnable) {
          jsonSource = source.map { x =>
            ProtobufUtil.protobufToJSON(_kafkaProtoClass.asInstanceOf[Class[Message]], x.value().asInstanceOf[Array[Byte]])
          }
        } else {
          jsonSource = source.map(_.value().asInstanceOf[String])
        }

        val filtered = jsonSource

        if (isFastPollingEnable && filtered.isEmpty() && GlobalAppRunningStatusV2.isPreviousRunning(concurrentGroup, moduleName)) {
          LOG.warn("Fast polling", "concurrentGroup", concurrentGroup, "moduleName", moduleName)
          moduleTracer.trace("fast polling")
          moduleReadingKafkaMarks.put(moduleName, false)

        } else {

          var dwi = hiveContext
            .read
            .schema(dwiStructType)
            .json(filtered)

          if (isEnableDwiUuid) {
            dwi = dwi.select(concat_ws(dwiUuidFieldsSeparator, dwiUuidFields.map { x => expr(x) }: _*).as(dwiUuidFieldsAlias), col("*"))
          } else {
            dwi = dwi.select(expr(s"null as $dwiUuidFieldsAlias"), col("*"))
          }

          dwi = dwi.alias("dwi").cache()//.persist(StorageLevel.MEMORY_ONLY_SER)
          dwi.count()
          moduleTracer.trace("read kafka")
          moduleReadingKafkaMarks.put(moduleName, false)

          //-----------------------------------------------------------------------------------------------------------------
          // Wait checking concurrent group status
          //-----------------------------------------------------------------------------------------------------------------
          //          LOG.warn(s"Show modules running status", "concurrentGroup", concurrentGroup, "moduleName", moduleName , "allModulesStatus", OM.toJOSN(GlobalAppRunningStatusV2.STATUS_MAP_AS_JAVA))
          moduleTracer.trace("wait module queue", {
            GlobalAppRunningStatusV2.waitRunAndSetRunningStatus(concurrentGroup, moduleName)
          })

          val groupName = if (StringUtil.notEmpty(dwrTable)) dwrTable else moduleName

          // 开始异步处理
          //          executorService.execute(new Runnable {
          new Thread(new Runnable {

            override def run(): Unit = {

              try {

                moduleTracer.startBatch

                //-----------------------------------------------------------------------------------------------------------------
                //  Begin Transaction !!
                //-----------------------------------------------------------------------------------------------------------------
                var parentTid = mixTransactionManager.beginTransaction(moduleName, groupName)

                var dwiLTimeExpr = s"'${mixTransactionManager.dwiLoadTime(moduleConfig)}'"
                var dwrLTimeExpr = s"'${mixTransactionManager.dwrLoadTime(moduleConfig)}'"
                LOG.warn("dwiLTimeExpr, dwrLTimeExpr", s"$dwiLTimeExpr, $dwrLTimeExpr")

                // 待删
                var uuidDwi: DataFrame = null

                //-----------------------------------------------------------------------------------------------------------------
                //  DWI Handler
                //-----------------------------------------------------------------------------------------------------------------
                //              var handledDwi = uuidDwi
                var handledDwi = dwi
                if (dwiHandlers != null) {
                  dwiHandlerCookies = Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()
                  dwiHandlers.foreach { case (_, _, handler) =>
                    //Key code !!
                    val (_handleDwi, cs) = handler.handle(handledDwi)
                    handledDwi = _handleDwi
                    dwiHandlerCookies :+= (handler, cs)
                    moduleTracer.trace(s"dwi ${handler.getClass.getSimpleName} handle")
                  }
                }

                var dwrDwi = handledDwi
                // Group by, Do not contain duplicate
                if (isEnableDwr) {
                  var filteredDwi: DataFrame = dwrDwi

                  //-----------------------------------------------------------------------------------------------------------------
                  //  DWR prepare handle
                  //-----------------------------------------------------------------------------------------------------------------
                  if (dwrHandlers != null && dwrHandlers.nonEmpty) {
//                    if (!isMaster && dwrHandlers.size == 1 && !dwrHandlers.head.isInstanceOf[UUIDFilterDwrHandler]) {
                    if (!isMaster && dwrHandlers.size > 0) {
                      LOG.warn(dwrHandlers.head.getClass.getName)
                      throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                    }
                    dwrHandlers.foreach { h =>
                      filteredDwi = h.prepare(filteredDwi)
                      LOG.warn(s"dwr ${h.getClass.getSimpleName} filter completed")
                      moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} filter")
                    }
                  }

                  val dwiFs = filteredDwi.schema.fieldNames
//                  val dwrGroupBy = dwrGroupByExprs.map{ x => x._2 }
                  val dwr = filteredDwi.selectExpr(dwiFs.map{ x => if (dwrGroupByOverwrite.contains(x)) s"${dwrGroupByOverwrite.get(x)._1} as ${dwrGroupByOverwrite.get(x)._2}" else x}: _*)
                    .withColumn("l_time", expr(dwrLTimeExpr))
                    .withColumn("b_date", to_date(expr(businessTimeExtractBy)).cast("string"))
                    .withColumn("b_time", expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwrBTimeFormat')").as("b_time"))
                    .groupBy(col("l_time") :: col("b_date") :: col("b_time") :: dwrGroupByExprs: _*)
                    .agg(aggExprs.head, aggExprs.tail: _*)

                  //-----------------------------------------------------------------------------------------------------------------
                  // Wait union all module dwr data
                  //-----------------------------------------------------------------------------------------------------------------
                  moduleTracer.trace("wait dwr union all", {
                    mixModulesBatchController.waitUnionAll(dwr, isMaster, moduleName)
                  })

                  //-----------------------------------------------------------------------------------------------------------------
                  //  DWR handle
                  //-----------------------------------------------------------------------------------------------------------------
                  if (dwrHandlers != null && dwrHandlers.nonEmpty) {
//                    if (!isMaster && dwrHandlers.size == 1 && !dwrHandlers.head.isInstanceOf[UUIDFilterDwrHandler]) {
                    if (!isMaster && dwrHandlers.size > 0) {
                      LOG.warn(dwrHandlers.head.getClass.getName)
                      throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                    }
                    dwrHandlers.foreach { x =>
                      mixModulesBatchController.set({
                        x.handle(mixModulesBatchController.get())
                      })
                      LOG.warn(s"Dwr ${x.getClass.getSimpleName} handle completed")
                      moduleTracer.trace(s"dwr ${x.getClass.getSimpleName} handle")
                    }
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Persistence
                //-----------------------------------------------------------------------------------------------------------------
                val dwiCount = offsetDetail.map(_._2).sum
                LOG.warn("dwiCount count", dwiCount)

                var uuidT, dwrT, dwiT, dwiPhoenixT, kafkaT, dmT: TransactionCookie = null
                var dwrAccDayT, dwrAccMonthT: TransactionCookie = null
                var dwrMySQLT, dwrMySQLDayT, dwrMySQLMonthT, dwrMySQLYearT: TransactionCookie = null
                var dwrClickHouseT, dwrClickHouseDayT: TransactionCookie= null
                val transactionCookies: Array[TransactionCookie] = null

                var ts = Array("repeated", "l_time", "b_date", "b_time")
                var iPs: Array[Array[HivePartitionPart]] = null
                var rPs: Array[Array[HivePartitionPart]] = null
//                var rPs_accday: Array[Array[HivePartitionPart]] = null
//                var rPs_accmonth: Array[Array[HivePartitionPart]] = null

                //-----------------------------------------------------------------------------------------------------------------
                // DWI partitions count
                //-----------------------------------------------------------------------------------------------------------------
                if (isEnableDwi) {
                  iPs = handledDwi
                    .dropDuplicates(ts)
                    .collect()
                    .map { x =>
                      ts.map { y =>
                        HivePartitionPart(y, x.getAs[String](y))
                      }
                    }
                  LOG.warn("count dwi partitions")
                  moduleTracer.trace("count dwi partitions")
                }

                //-----------------------------------------------------------------------------------------------------------------
                // DWR partitions count
                //-----------------------------------------------------------------------------------------------------------------
                ts = Array("l_time", "b_date", "b_time")
                if (isEnableDwr && isMaster) {

                  val df = mixModulesBatchController.get()
                  rPs = df
                    .dropDuplicates(ts)
                    .collect()
                    .map { x =>
                      ts.map { y =>
                        HivePartitionPart(y, x.getAs[String](y))
                      }
                    }
                  LOG.warn("count dwr partitions")
                  moduleTracer.trace("count dwr partitions")

//                  if (needDwrAccDay) {
//                    rPs_accday = convertRPsToDayFormat(rPs)
//                  }
//
//                  if (needDwrAccMonth) {
//                    rPs_accmonth = convertRPsToMonthFormat(rPs)
//                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                // DWR Hive Persistence
                //-----------------------------------------------------------------------------------------------------------------
                // 线程池开始
                ThreadPool.concurrentExecuteStatic({
                  if (isEnableDwr && dwiCount > 0) {
                    if (isMaster) {
                      LOG.warn("master start to calc new data to hive")
                      var dwrFields = mixModulesBatchController.get().schema.fieldNames
                      moduleTracer.startBatch

                      ThreadPool.concurrentExecuteStatic({
                        moduleTracer.startBatch
                        dwrT = hiveClient.overwriteUnionSum (
                          parentTid,
                          dwrTable,
                          mixModulesBatchController.get(),
                          aggExprsAlias,
                          unionAggExprsAndAlias,
                          Set[String](),
                          dwrGroupByExprsAlias, //dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias,
                          rPs,
                          null,
                          "l_time",
                          "b_date",
                          "b_time"
                        )

                        LOG.warn("hiveClient.overwriteUnionSum dwrTable completed", dwrT)
                        moduleTracer.trace("hive dwr save")
                      }, {
                        if (needDwrAccDay) {
                          moduleTracer.startBatch
                          var overwriteFields: java.util.Map[String, String] = new util.HashMap[String, String]()
                          try {
                            overwriteFields = config.getConfigList(s"modules.$moduleName.dwr.acc.day.overwrite").map { x =>
                              x.getString("as") -> x.getString("expr")
                            }.toMap.asJava
                            LOG.warn("overwriteFields", overwriteFields)
                          } catch {
                            case e: Exception => LOG.warn("get accday overwrite fields failed")
                          }
                          val fields = dwrFields.map { x =>
                            if (x.equals("l_time")) {
                              "date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time"
                            } else if (x.equals("b_date")) {
                              "date_format(b_date, 'yyyy-MM-dd') as b_date"
                            } else if (x.equals("b_time")) {
                              "date_format(b_time, 'yyyy-MM-dd 00:00:00') as b_time"
                            } else if (overwriteFields.contains(x)) {
                              s"""${overwriteFields.get(x)} as $x"""
                            } else {
                              x
                            }
                          }
                          LOG.warn("after overwrite fields", fields)
                          try {
                            dwrAccDayT = hiveClient.overwriteUnionSum(
                              parentTid,
                              s"${dwrTable}_accday",
                              mixModulesBatchController.get().selectExpr(fields: _*),
                              aggExprsAlias,
                              unionAggExprsAndAlias,
                              Set[String](),
                              dwrGroupByExprsAlias,
//                              rPs_accday,
                              null,
                              "l_time",
                              "b_date",
                              "b_time"
                            )
                          } catch { case e: Exception =>
                            LOG.warn("overwriteUnionSum acc day error")
                            throw new RuntimeException(s"module $moduleName overwrite union sum error at time ${new Date().toString}")
                          }

                          moduleTracer.trace("hive acc day dwr save")
                        }
                      }, {
                        if (needDwrAccMonth) {
                          moduleTracer.startBatch
                          var overwriteFields: java.util.Map[String, String] = new util.HashMap[String, String]()
                          try {
                            overwriteFields = config.getConfigList(s"modules.$moduleName.dwr.acc.month.overwrite").map { x =>
                              // as为字段名，expr为值
                              x.getString("as") -> x.getString("expr")
                            }.toMap.asJava
                          } catch {
                            case e: Exception => LOG.warn("get accmonth overwrite field failed")
                          }
                          try {
                            dwrAccMonthT = hiveClient.overwriteUnionSum(
                              parentTid,
                              s"${dwrTable}_accmonth",
                              mixModulesBatchController.get().selectExpr(
                                dwrFields.map { x =>
                                  if (x.equals("l_time")) {
                                    "date_format(l_time, 'yyyy-MM-01 00:00:00') as l_time"
                                  } else if (x.equals("b_date")) {
                                    "date_format(b_date, 'yyyy-MM-01') as b_date"
                                  } else if (x.equals("b_time")) {
                                    "date_format(b_time, 'yyyy-MM-01 00:00:00') as b_time"
                                  } else if (overwriteFields.contains(x)) {
                                    s"""${overwriteFields.get(x)} as $x"""
                                  } else {
                                    x
                                  }
                                }: _*
                              ),
                              aggExprsAlias,
                              unionAggExprsAndAlias,
                              Set[String](),
                              dwrGroupByExprsAlias, //dwrGroupByExprsAlias ++ dwrGroupbyExtendedFieldsAlias,
//                              rPs_accmonth,
                              null,
                              "l_time",
                              "b_date",
                              "b_time"
                            )
                          } catch {
                            case e: Exception =>
                              LOG.warn("overwriteUnionSum acc month error")
                              throw new RuntimeException(s"module $moduleName overwrite union sum error at time ${new Date().toString}")
                          }
                          moduleTracer.trace("hive acc month dwr save")
                        }
                      }, {
                        if (clickhousePersis) {
                          moduleTracer.startBatch
                          LOG.warn(s"ClickHouseClient overwriteUnionSum dwrTable start")
                          dwrClickHouseT = clickHouseClient.overwriteUnionSum(
                            parentTid,
                            dwrTable,
                            clickhousePeresisTable,
                            mixModulesBatchController.get(),
                            rPs,
                            "l_time",
                            "b_date",
                            "b_time"
                          )
                          LOG.warn("clickHouseClient overwriteUnionSum dwrTable completed", dwrClickHouseT)
                          moduleTracer.trace("clickhosue dwr save")
                        }
                      })
                      moduleTracer.trace("hive all dwr save")
                    }
                  }
                }, {
                  //-----------------------------------------------------------------------------------------------------------------
                  // DWI Hive Persistence
                  //-----------------------------------------------------------------------------------------------------------------
                  if (isEnableDwi && dwiCount > 0) {
                    moduleTracer.startBatch
                    dwiT = hiveClient.into(
                      parentTid,
                      dwiTable,
                      handledDwi,
                      iPs
                    )
                    LOG.warn("hiveClient.into dwiTable completed", dwiT)
                    moduleTracer.trace("hive dwi save")
                    //          traceBatchUsingTime("hvie dwi save", lastTraceTime, traceBatchUsingTimeLog)
                  }
                })
                // 线程池结束

                if (isMaster && needH2Persistence) {
                  // create table if not exists
                  var fields = h2PersistFields.map{x => s"${x._1} as ${x._2}"} ++ h2PersistAggFields.map{ x => x._2}
                  var persistData = hiveContext.read.table(dwrTable).selectExpr(fields:_*)
                  val h2Fields = persistData.schema.fields.map{ x =>
                    s"${x.name} ${x.dataType.simpleString}"
                  }
                  val sqlBuilder = new mutable.StringBuilder()
                  sqlBuilder.append("CREATE MEMORY TABLE IF NOT EXISTS ").append(h2TableName).append("(")
                  sqlBuilder.append(h2Fields.mkString(", ")).append(");")
                  LOG.warn("h2 create table", s"create table sql: \n" + sqlBuilder.toString())
                  h2JDBCClient.execute(sqlBuilder.toString())

                  // persistence in h2
                  var data = persistData
                    .groupBy(h2PersistFields.map{ x => x._1}.head, h2PersistFields.map{ x => x._1}.tail:_*)
                    .agg(h2PersistAggFields.map{ x => expr(x._1).as(x._2)}.head, h2PersistAggFields.map{ x => expr(x._1).as(x._2)}.tail:_*)
                    .collect()
                  LOG.warn("h2 data length", s"data length = ${data.length}")
                  fields = h2PersistFields.map{x => s"${x._2}"} ++ h2PersistAggFields.map{ x => x._2}
                  h2JDBCClient.executeBatch(data.map{ x=>
                    val insertSQL = s"""INSERT INTO $h2TableName(${fields.mkString(", ")}) VALUES(${x.mkString(",")})"""
                    LOG.warn("h2 insert", "insert sql: \n" + insertSQL)
                    insertSQL
//                    s"""INSERT INTO $h2TableName(${fields.mkString(", ")}) VALUES(${x.mkString(",")})"""
                  }:_*)
                }


                //-----------------------------------------------------------------------------------------------------------------
                // Kafka set offset
                //-----------------------------------------------------------------------------------------------------------------
                kafkaT = kafkaClient.setOffset(parentTid, offsetRanges)
                LOG.warn("kafkaClient.setOffset completed", kafkaT)
                //每半个小时保存一次offset消息 便于异常回滚之前某个时刻的offset
                val updateTime = CSTTime.now.modifyMinuteAsTime(-30)
                val KEEP_OFFSET_CER = s"${moduleName}_keep_offset_cer"
                val KEEP_OFFSET_TOPIC = s"${moduleName}_keep_offset_topic"
                val latestPartitionOffsetTopic = s"${moduleName}_partition_offset_topic"

                  MC.pull(KEEP_OFFSET_CER, Array(KEEP_OFFSET_TOPIC), { x =>

                  //            val lastTime  = if(!x.isEmpty && x.size > 1) x.tail.map(_.getKeyBody).head else x.map(_.getKeyBody).head
                  val lastTime = if (!x.isEmpty) x.reverse.map(_.getKeyBody).take(1).head else ""

                  val needUpdate = x.isEmpty || (CSTTime.date2TimeStamp(lastTime) <= CSTTime.date2TimeStamp(updateTime) /*!x.map(_.getKeyBody).contains(updateTime)*/)

                  LOG.warn("keep kafka offset", "updateTime", updateTime, "lastTime", lastTime, "KEEP_OFFSET_CER", KEEP_OFFSET_CER, "needUpdate", needUpdate)

                  if (needUpdate) {
                    val data = offsetRanges
                      .map { o =>
                        LatestOffsetRecord(moduleName, o.topic, o.partition, o.untilOffset)
                      }

                    MC.push(PushReq(latestPartitionOffsetTopic, OM.toJOSN(data)))


                    MC.push(PushReq(KEEP_OFFSET_TOPIC, CSTTime.now.time()))

                  }

                  false
                })

                //-----------------------------------------------------------------------------------------------------------------
                // DWI HBase Persistence
                //-----------------------------------------------------------------------------------------------------------------
                if (dwiPhoenixEnable && dwiCount > 0) {

                  val c = Class.forName(dwiPhoenixHBaseStorableClass).asInstanceOf[Class[_ <: HBaseStorable]]
                  val dwiP = handledDwi
                    .toJSON
                    .rdd
                    .map { x =>
                      OM.toBean(x, c)
                    }

                  LOG.warn("hbaseClient putsNonTransaction start, take(2)", dwiP.take(2))

                  if (dwiPhoenixSubtableEnable) {
                    hbaseClient.putsNonTransactionMultiSubTable(
                      dwiPhoenixTable,
                      dwiP
                    )
                  } else {
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

                  LOG.warn("hbaseClient putsNonTransaction done")
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Cache Current RollbackableTransactionCookie
                //-----------------------------------------------------------------------------------------------------------------
                if (dwiHandlerCookies != null) {
                  dwiHandlerCookies.foreach { x =>
                    x._2.foreach { y =>
                      cacheTransactionCookies(COOKIE_KIND_DWI_EXT_FIELDS_T, y)
                    }
                  }
                }
                cacheTransactionCookies(COOKIE_KIND_UUID_T, uuidT)
                cacheTransactionCookies(COOKIE_KIND_DWI_T, dwiT)
                cacheTransactionCookies(COOKIE_KIND_DWR_T, dwrT)
                cacheTransactionCookies(COOKIE_KIND_DWR_ACC_DAY_T, dwrAccDayT)
                cacheTransactionCookies(COOKIE_KIND_DWR_ACC_MONTH_T, dwrAccMonthT)
//                cacheTransactionCookies(COOKIE_KIND_DWR_MYSQL_DAY_T, dwrMySQLDayT)
//                cacheTransactionCookies(COOKIE_KIND_DWR_MYSQL_MONTH_T, dwrMySQLMonthT)
                cacheTransactionCookies(COOKIE_KIND_DWR_CLICKHOUSE_T, dwrClickHouseT)
                cacheTransactionCookies(COOKIE_KIND_DWR_CLICKHOUSE_DAY_T, dwrClickHouseDayT)
                cacheTransactionCookies(COOKIE_KIND_KAFKA_T, kafkaT)
                cacheTransactionCookies(COOKIE_KIND_DM_T, dmT)

                //------------------------------------------------------------------------------------
                // Commit Transaction
                //------------------------------------------------------------------------------------
                if (isEnableDwr && isMaster && dwiCount > 0) {
                  //                  val threadPool = new ThreadPool()
                  // 线程池开始
                  ThreadPool.concurrentExecuteStatic({
                    moduleTracer.startBatch
                    hiveClient.commit(dwrT)
                    moduleTracer.trace("hive dwr commit")
                  }, {
                    if (needDwrAccDay) {
                      moduleTracer.startBatch
                      hiveClient.commit(dwrAccDayT)
                      moduleTracer.trace("hive acc day dwr commit")
                    }
                  }, {
                    if (needDwrAccMonth) {
                      moduleTracer.startBatch
                      hiveClient.commit(dwrAccMonthT)
                      moduleTracer.trace("hive acc month dwr commit")
                    }
                  }, {
                    if (clickhousePersis) {
                      moduleTracer.startBatch
                      clickHouseClient.commit(dwrClickHouseT)
                      moduleTracer.trace("clickhouse dwr commit")
                    }
                  })
                  moduleTracer.trace("Dwr all commit")

                  // 线程池结束
                  LOG.warn("hiveClient dwr committed", dwrT)
                }

                if (isEnableDwi && dwiCount > 0) {
                  hiveClient.commit(dwiT)
                  LOG.warn("hiveClient dwi committed", dwiT)
                  moduleTracer.trace("hive dwi commit")
                }

                if (dwiHandlerCookies != null && dwiHandlerCookies.length > 0) {
                  dwiHandlerCookies.foreach { x =>
                    x._2.foreach { y =>
                      x._1.commit(y)
                      LOG.warn(s"${x._1.getClass.getSimpleName}.commit() completed", y)
                    }
                  }
                  moduleTracer.trace("dwi handler commit")
                }


                kafkaClient.commit(kafkaT)
                LOG.warn("kafkaClient offset committed", kafkaT)
                moduleTracer.trace("kafka commit")

                //------------------------------------------------------------------------------------
                // Push partitions message
                //------------------------------------------------------------------------------------
                pushChangedHivePartitin(dwrT.asInstanceOf[HiveTransactionCookie], dwiT.asInstanceOf[HiveTransactionCookie])

                if (isEnableHandlerDm && dmHandlers != null) {
                  dmHandlers.foreach { x =>
                    LOG.warn(s"dm handler(${x.getClass.getSimpleName}) start")
                    x.handle()
                    LOG.warn(s"dm handler(${x.getClass.getSimpleName}) done")
                  }
                  moduleTracer.trace("dm handler")
                }

                if (isEnablePhoenixDm) {
                  phoenixClient.commit(dmT)
                  LOG.warn("phoenixClient.commit(t5) completed(upsertUnionSum cacheGroupByDwr)", dmT)
                }

                //------------------------------------------------------------------------------------
                // DWR send to kafka
                //------------------------------------------------------------------------------------
                if (isEnableKafkaDm && isMaster && dwiCount > 0) {
                  val k = mixModulesBatchController.get().selectExpr(
                    dwrGroupByExprsAlias ++ /*dwrGroupbyExtendedFieldsAlias ++*/
                      aggExprsAlias :+
                      s"l_time as dwrLoadTime" :+
                      s"b_date as dwrBusinessDate" :+
                      s"b_time as dwrBusinessTime": _*)
                    .toJSON
                    .collect()

                  LOG.warn(s"kafkaClient sendToKafka $dmKafkaTopic start, count", k.length)

                  kafkaClient.sendToKafka(dmKafkaTopic, k: _*)
                  LOG.warn(s"kafkaClient.sendToKafka $dmKafkaTopic done, take(2)", k.take(2))
                  moduleTracer.trace("dwr to kafka")
                }

                //------------------------------------------------------------------------------------
                // Wait all module transaction commit
                //------------------------------------------------------------------------------------
                LOG.warn(s"wait transaction commit start", "tid", parentTid, "master", isMaster)
                // Key code !!
                moduleTracer.trace("wait master tx commit", {
                  mixTransactionManager.commitTransaction(isMaster, moduleName, {
//                    mixTransactionManager.commitTransaction0(isMaster, parentTid, moduleName)
                    mixModulesBatchController.completeBatch(isMaster)
                  })
                })
                LOG.warn(s"wait transaction commit done")

                //------------------------------------------------------------------------------------
                // Clean
                //------------------------------------------------------------------------------------
                if (dwi != null) dwi.unpersist()
                if (uuidDwi != null) uuidDwi.unpersist()
                dwiHandlerCookies = null

                // 清理上一次启动的产生的事务数据
                if (!mixTransactionManager.needTransactionalAction()) {
                  if (hiveCleanable != null) {
                    hiveCleanable.doActions()
                    hiveCleanable = null
                  }
                  if (mysqlCleanable != null) {
                    mysqlCleanable.doActions()
                    mysqlCleanable = null
                  }
                  if (clickHouseCleanable != null) {
                    clickHouseCleanable.doActions()
                    clickHouseCleanable = null
                  }
                  if (hbaseCleanable != null) {
                    hbaseCleanable.doActions()
                    hbaseCleanable = null
                  }
                  if (kafkaCleanable != null) {
                    kafkaCleanable.doActions()
                    kafkaCleanable = null
                  }
                  if (phoenixCleanable != null) {
                    phoenixCleanable.doActions()
                    phoenixCleanable = null
                  }
                }

                if (isEnableDwr && isMaster && dwiCount > 0) {
                  hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWR_T, parentTid): _*)
                  hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWR_ACC_DAY_T, parentTid): _*)
                  hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWR_ACC_MONTH_T, parentTid): _*)
                  moduleTracer.trace("clean hive dwr")

                  if (clickhousePersis) {
                    clickHouseClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWR_CLICKHOUSE_T, parentTid): _*)
                  }
                  moduleTracer.trace("clean clickhouse dwr")

                }
                if (isEnableDwi && dwiCount > 0) {
                  hiveClient.clean(popNeedCleanTransactions(COOKIE_KIND_DWI_T, parentTid): _*)
                  moduleTracer.trace("clean hive dwi")
                }
                if (dwiHandlerCookies != null) {
                  dwiHandlerCookies.foreach { x =>
                    x._2.foreach { y =>
                      x._1.clean(popNeedCleanTransactions(COOKIE_KIND_DWI_EXT_FIELDS_T, parentTid): _*)
                      moduleTracer.trace("clean dwi handler")
                    }
                  }
                }

                kafkaClient.clean(popNeedCleanTransactions(COOKIE_KIND_KAFKA_T, parentTid): _*)
                moduleTracer.trace("clean kafka")

                if (isEnablePhoenixDm) phoenixClient.clean(popNeedCleanTransactions(COOKIE_KIND_DM_T, parentTid): _*)

                LOG.warn(moduleName, "clean kafka and set module status idle")
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
                     |    ${(100.0 * config.getInt("spark.conf.streaming.batch.buration") / 60).asInstanceOf[Int] / 100.0},
                     |    "${moduleTracer.getHistoryBatchesTraceResult()}",
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

                tryKillSelfApp()

              } catch {
                case e: Throwable =>
                  LOG.warn(s"Kill self yarn app via async thread error !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName, "error", e)
                  YarnAPPManagerUtil.killApps(appName)
              }

            }
          }).start()
        }
      } catch {
        case e: Exception => throw new ModuleException(s"${classOf[FasterModule].getSimpleName} '$moduleName' execution failed !! ", e)
      }
    }
  }


  //<cookieKind, <transactionParentId, transactionCookies>>
  private def cacheTransactionCookies(cookieKind: String, transactionCookie: TransactionCookie): Unit = {

    if (transactionCookie.isInstanceOf[HiveRollbackableTransactionCookie]
      || transactionCookie.isInstanceOf[KafkaRollbackableTransactionCookie]
      || transactionCookie.isInstanceOf[HBaseTransactionCookie]
      || transactionCookie.isInstanceOf[MySQLRollbackableTransactionCookie]
      || transactionCookie.isInstanceOf[ClickHouseRollbackableTransactionCookie]
    ) {

      var pr = batchsTransactionCookiesCache.get(cookieKind)
      if (pr == null) {
        pr = new util.ArrayList[TransactionCookie]()
        batchsTransactionCookiesCache.put(cookieKind, pr)
      }
      pr.add(transactionCookie)
    }
  }

  private val EMPTY_TRANSACTION_COOKIES = Array[TransactionCookie]()

  //找到并移除
  private def popNeedCleanTransactions(cookieKind: String, excludeCurrTransactionParentId: String): Array[TransactionCookie] = {
    var result = EMPTY_TRANSACTION_COOKIES
    var pr = batchsTransactionCookiesCache.get(cookieKind)
    var isTran = mixTransactionManager.needTransactionalAction()
    if (pr != null && isTran) {
      var needCleans = pr.filter(!_.parentId.equals(excludeCurrTransactionParentId))
      pr.removeAll(needCleans)
      result = needCleans.toArray
    }
    result
  }

  private def pushChangedHivePartitin(dwrT: HiveTransactionCookie, dwiT: HiveTransactionCookie): Unit = {

    var topic: String = null

    if (dwiT != null && dwiT.partitions != null && dwiT.partitions.nonEmpty) {
      topic = dwiT.targetTable

      val key = OM.toJOSN(dwiT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

      topic = moduleName
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

    } else {
      LOG.warn(s"MessageClient dwi no hive partitions to push", s"topic: $topic")
    }

    if (dwrT != null && dwrT.partitions != null && dwrT.partitions.nonEmpty) {
      val topic = dwrT.targetTable
      var key = OM.toJOSN(dwrT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")
    } else {
      LOG.warn(s"MessageClient dwr no hive partitions to push", s"topic: $topic")
    }

  }

//  def convertRPsToDayFormat(rPs: Array[Array[HivePartitionPart]]): Array[Array[HivePartitionPart]] = {
//    val filter = new util.HashSet[String]()
//    val timeFormatter = CSTTime.formatter("yyyy-MM-dd 00:00:00")
//    rPs.map { ps =>
//      ps.map { p =>
//        if (("l_time".equals(p.getName) || "b_time".equals(p.getName)) && p.getValue.length > 0) {
//          HivePartitionPart(p.getName, timeFormatter.format(CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse(p.getValue)))
//        } else {
//          HivePartitionPart(p.getName, p.getValue)
//        }
//      }.filter { p =>
//        filter.add(p.getName + p.getValue)
//      }
//    }.filter { p => p.length > 0 }
//  }
//
//  def convertRPsToMonthFormat(rPs: Array[Array[HivePartitionPart]]): Array[Array[HivePartitionPart]] = {
//    val filter = new util.ArrayList[Integer]()
//    val timeFormatter = CSTTime.formatter("yyyy-MM-01 00:00:00")
//    val dayFormatter = CSTTime.formatter("yyyy-MM-01")
//
//    rPs.map { ps =>
//      ps.map { p =>
//        if (("l_time".equals(p.getName) || "b_time".equals(p.getName)) && p.getValue.length > 0) {
//          HivePartitionPart(p.getName, timeFormatter.format(CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse(p.getValue)))
//        } else if ("b_date".equals(p.getName) && p.getValue.length > 0) {
//          HivePartitionPart(p.getName, dayFormatter.format(CSTTime.formatter("yyyy-MM-dd").parse(p.getValue)))
//        } else {
//          HivePartitionPart(p.getName, p.getValue)
//        }
//      }.filter { p =>
//        var stay = true
//        stay = filter.contains(p.getName.hashCode + p.getValue.hashCode)
//        filter.add(p.getName.hashCode + p.getValue.hashCode)
//        !stay
//      }.filter { p =>
//        !(p == null)
//      }
//    }.filter { p => p.length > 0 }
//  }

  def tryKillSelfApp(): Unit = {
    var killSelfApp = false
    MC.pull("kill_self_cer", Array(s"kill_self_$appName"), { x =>
      if (x.nonEmpty) {
        if (isMaster) {
          // 尽量等待所有module完成
          Thread.sleep(10 * 1000)
          killSelfApp = true
        } else {
          //等待master module kill自身app
          while (true) {
            Thread.sleep(2000)
          }
        }
        true
      } else {
        false
      }
    })

    if (killSelfApp) {
      LOG.warn(s"Kill self yarn app via user operation !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName)
      YarnAPPManagerUtil.killApps(appName)
    }
  }

  def runInTry(func: => Unit) {
    try {
      func
    } catch {
      case e: Exception =>
    }
  }

  def runInTry(func: => Unit, catchFunc: => Unit): Unit = {
    try {
      func
    } catch {
      case e: Exception =>
        catchFunc
    }
  }

  def stop(): Unit = {
    try {
      if (stream != null) stream.stop()
    } catch {
      case e: Exception =>
        LOG.error(s"${getClass.getName} '$moduleName' stop fail!", e)
    }
  }

}
