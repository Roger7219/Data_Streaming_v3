package com.mobikok.ssp.data.streaming.module

import java.text.DecimalFormat
import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.google.protobuf.Message
import com.mobikok.message.MessagePushReq
import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.MonitorClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{LatestOffsetRecord, OffsetRange}
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.Handler
import com.mobikok.ssp.data.streaming.handler.dm.offline.{ClickHouseQueryByBDateHandler, ClickHouseQueryByBTimeHandler, ClickHouseQueryMonthHandler}
import com.mobikok.ssp.data.streaming.handler.dwi.core.{HBaseDWIPersistHandler, HiveDWIPersistHandler, UUIDFilterDwiHandler}
import com.mobikok.ssp.data.streaming.handler.dwr.UUIDFilterDwrHandler
import com.mobikok.ssp.data.streaming.handler.dwr.core.{HiveDWRPersistDayHandler, HiveDWRPersistHandler, HiveDWRPersistMonthHandler}
import com.mobikok.ssp.data.streaming.module.support.uuid.{DefaultUuidFilter, UuidFilter}
import com.mobikok.ssp.data.streaming.module.support.{HiveContextGenerater, MixModulesBatchController, SQLContextGenerater}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat_ws, expr, to_date}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.collection.JavaConversions._
import scala.collection.mutable

class PluggableModule(config: Config,
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

  val dwiUuidFieldsSeparator = "^"

  //moduleName, transactionCookies

//  val COOKIE_KIND_UUID_T = "uuidT"
//  val COOKIE_KIND_DWI_EXT_FIELDS_T = "dwiExtFieldsT"
//  val COOKIE_KIND_DWI_T = "dwiT"
//  val COOKIE_KIND_DWR_T = "dwrT"
//  val COOKIE_KIND_DWR_ACC_DAY_T = "dwrAccDayT"
//  val COOKIE_KIND_DWR_ACC_MONTH_T = "dwrAccMonthT"
//  val COOKIE_KIND_DWR_MYSQL_DAY_T = "dwrMySQLDayT"
//  val COOKIE_KIND_DWR_MYSQL_MONTH_T = "dwrMySQLMonthT"
//  val COOKIE_KIND_DWR_CLICKHOUSE_T = "dwrClickHouseT"
//  val COOKIE_KIND_DWR_CLICKHOUSE_DAY_T = "dwrClickHouseDayT"
//  //  val COOKIE_KIND_DWI_PHOENIX_T = "dwiPhoenixT"
  val COOKIE_KIND_KAFKA_T = "kafkaT"
//  val COOKIE_KIND_DM_T = "dmT"
//  val HBASE_WRITECOUNT_BATCH_TOPIC = "hbase_writecount_topic"

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
    case _: Exception =>
  }

  var aggExprsAlias: List[String] = _
  try {
    aggExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
  } catch {
    case _: Exception =>
  }

  var unionAggExprsAndAlias: List[Column] = _
  try {
    unionAggExprsAndAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var businessTimeExtractBy: String = _
  try {
    businessTimeExtractBy = config.getString(s"modules.$moduleName.business.time.extract.by")
  } catch {
    case _: Throwable =>
      //兼容历史代码
      businessTimeExtractBy = config.getString(s"modules.$moduleName.business.date.extract.by")
  }

  var isFastPollingEnable = false
  if (config.hasPath(s"modules.$moduleName.fast.polling.enable")) {
    isFastPollingEnable = config.getBoolean(s"modules.$moduleName.fast.polling.enable")
  }

  val monitorClient = new MonitorClient(messageClient)
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

  val isMaster = mixModulesBatchController.isMaster(moduleName)

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
  var hbaseClient = new HBaseMultiSubTableClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
  val hiveClient = new HiveClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, config, mixTransactionManager)
  val phoenixClient = new PhoenixClient(moduleName, ssc.sparkContext, config, mixTransactionManager, moduleTracer)
  //  val greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val greenplumClient = null.asInstanceOf[GreenplumClient]

//  var h2JDBCClient = null.asInstanceOf[H2JDBCClient]
//  try {
//    h2JDBCClient = new H2JDBCClient(h2JDBCConnectionUrl, "", "")
//  } catch {
//    case e: Throwable => LOG.warn("H2JDBCClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")
//  }

//  val mysqlClient = new MySQLClient(moduleName, ssc.sparkContext, config, messageClient, mixTransactionManager, moduleTracer)
  val clickHouseClient = new ClickHouseClient(moduleName, config, ssc, messageClient, mixTransactionManager, hiveContext, moduleTracer)
  //-------------------------  Clients End  -------------------------


  //-------------------------  Dwi about  -------------------------
  var dwiPhoenixEnable = false
  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable = null.asInstanceOf[String]
  var dwiPhoenixHBaseStorableClass = null.asInstanceOf[String]
  try {
    dwiPhoenixEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.enable")
  } catch {
    case _: Exception =>
  }
  if (dwiPhoenixEnable) {
    dwiPhoenixTable = config.getString(s"modules.$moduleName.dwi.phoenix.table")
    dwiPhoenixHBaseStorableClass = config.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
  }
  if (config.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")) {
    dwiPhoenixSubtableEnable = config.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
  }

  var isEnableDwi = false
  var dwiTable: String = _
  try {
    isEnableDwi = config.getBoolean(s"modules.$moduleName.dwi.enable")
  } catch {
    case _: Exception =>
  }
  try {
    dwiTable = config.getString(s"modules.$moduleName.dwi.table")
  } catch {
    case e: Throwable =>
      if (isEnableDwi) throw e
  }

//  var dwiHandlerCookies: Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])] = _ // Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()
  var dwiHandlers: List[com.mobikok.ssp.data.streaming.handler.dwi.Handler] = _
  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwiBTimeFormat = config.getString(s"modules.$moduleName.dwi.business.time.format.by")
  } catch {
    case _: Throwable =>
  }

  var isEnableDwiUuid = false
  var dwiUuidStatHbaseTable: String = _
  var dwiUuidFieldsAlias: String = "rowkey"
  var dwiUuidFields: Array[String] = _
  try {
    isEnableDwiUuid = config.getBoolean(s"modules.$moduleName.dwi.uuid.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableDwiUuid) {
    dwiUuidFields = config.getStringList(s"modules.$moduleName.dwi.uuid.fields").toArray[String](new Array[String](0))
    try {
      dwiUuidFieldsAlias = config.getString(s"modules.$moduleName.dwi.uuid.alias")
    } catch {
      case _: Exception =>
    }

    dwiUuidStatHbaseTable = dwiTable + "_uuid"
    try {
      dwiUuidStatHbaseTable = config.getString(s"modules.$moduleName.dwi.uuid.stat.hbase.table")
    } catch {
      case _: Exception =>
    }
  }

  var uuidFilter: UuidFilter = _
  if (config.hasPath(s"modules.$moduleName.uuid.filter.class")) {
    val f = config.getString(s"modules.$moduleName.uuid.filter.class")
    uuidFilter = Class.forName(f).newInstance().asInstanceOf[UuidFilter]
  } else {
    uuidFilter = new DefaultUuidFilter()
  }
  uuidFilter.init(moduleName, config, hiveContext, moduleTracer, dwiUuidFieldsAlias, businessTimeExtractBy, dwiTable)
  //-------------------------  Dwi End  -------------------------


  //-------------------------  Dwr about  -------------------------

  var isEnableDwr = false
  var dwrTable: String = _
  try {
    isEnableDwr = config.getBoolean(s"modules.$moduleName.dwr.enable")
  } catch {case _: Exception =>}
  if (isEnableDwr) {
    dwrTable = config.getString(s"modules.$moduleName.dwr.table")
  }

  var dwrGroupByExprs: List[Column] = _
  try {
    dwrGroupByExprs = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var dwrGroupByExprsAlias: Array[String] = _
  try {
    dwrGroupByExprsAlias = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray[String]
  } catch {
    case _: Exception =>
  }

  var dwrGroupByExprsAliasCol: List[Column] = _
  try {
    dwrGroupByExprsAliasCol = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => col(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwrBTimeFormat = config.getString(s"modules.$moduleName.dwr.business.time.format.by")
  } catch {
    case _: Throwable =>
  }

  var dwrIncludeRepeated = true
  if (config.hasPath(s"modules.$moduleName.dwr.include.repeated")) {
    dwrIncludeRepeated = config.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  } else if (isEnableDwiUuid) {
    dwrIncludeRepeated = false
  }

  var dwrHandlers: util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = _
  var dwrHandlerCookies: Array[(String, TransactionCookie)] = _
  if (isMaster) {
    dwrHandlers = new util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler]()
    if (!dwrIncludeRepeated) {
      dwrHandlers.add(new UUIDFilterDwrHandler(uuidFilter))
    }
    // 核心handler的手动配置
    if (isEnableDwr) {
      val dwrPersistHandler = new HiveDWRPersistHandler()
      dwrPersistHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, config, config, "", "")
      dwrHandlers.add(dwrPersistHandler)
    }

    var enableDwrAccDay = false
    runInTry{enableDwrAccDay = config.getBoolean(s"modules.$moduleName.dwr.acc.day.enable")}
    if (enableDwrAccDay) {
      val dwrPersistDayHandler = new HiveDWRPersistDayHandler()
      dwrPersistDayHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, config, config, "", "")
      dwrHandlers.add(dwrPersistDayHandler)
    }

    var enableDwrAccMonth = false
    runInTry{enableDwrAccMonth = config.getBoolean(s"modules.$moduleName.dwr.acc.month.enable")}
    if (enableDwrAccMonth) {
      val dwrPersistMonthHandler = new HiveDWRPersistMonthHandler()
      dwrPersistMonthHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, config, config, "", "")
      dwrHandlers.add(dwrPersistMonthHandler)
    }
    try {
      config.getConfigList(s"modules.$moduleName.dwr.handler").foreach { x =>
        val hc = x
        val h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
        val expr = if (x.hasPath("expr")) x.getString("expr") else ""
        val as = if (x.hasPath("as")) x.getString("as") else ""
        h.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, hc, config, expr, as)
        dwrHandlers.add(h)
      }
    } catch {
      case e: Exception => LOG.warn(s"Dwr init error, exception: ${e.getMessage}")
    }
  }

  //-------------------------  Dwr End  -------------------------

  //-------------------------  Dm about  -------------------------
  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(config.getString(s"modules.$moduleName.dm.load.time.format.by"))
  } catch {
    case _: Exception =>
  }

//  var isEnableKafkaDm = false
//  var dmKafkaTopic: String = _
//  try {
//    isEnableKafkaDm = config.getBoolean(s"modules.$moduleName.dm.kafka.enable")
//  } catch {
//    case _: Exception =>
//  }
//  if (isEnableKafkaDm) {
//    dmKafkaTopic = config.getString(s"modules.$moduleName.dm.kafka.topic")
//  }

//  var isEnablePhoenixDm = false
//  var dmPhoenixTable: String = _
//  var dmHBaseStorableClass: Class[_ <: HBaseStorable] = _
//  try {
//    isEnablePhoenixDm = config.getBoolean(s"modules.$moduleName.dm.phoenix.enable")
//  } catch {
//    case _: Exception =>
//  }
//  if (isEnablePhoenixDm) {
//    dmPhoenixTable = config.getString(s"modules.$moduleName.dm.phoenix.table")
//    dmHBaseStorableClass = Class.forName(config.getString(s"modules.$moduleName.dm.phoenix.hbase.storable.class")).asInstanceOf[Class[_ <: HBaseStorable]]
//  }

  var isEnableOfflineDm = false
  var dmOfflineHandlers: List[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler] = _
  try {
    isEnableOfflineDm = config.getBoolean(s"modules.$moduleName.dm.offline.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableOfflineDm) {
    try {
      dmOfflineHandlers = config.getConfigList(s"modules.$moduleName.dm.offline.handlers").map { setting =>
        val h = Class.forName(setting.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]
        h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClient, hbaseClient, hiveContext, setting)
        if (h.isInstanceOf[ClickHouseQueryByBTimeHandler] || h.isInstanceOf[ClickHouseQueryByBDateHandler] || h.isInstanceOf[ClickHouseQueryMonthHandler]) {
          h.setClickHouseClient(clickHouseClient)
        }
        h
      }.toList
    } catch {
      case _: Exception =>
    }
  }

  var isEnableOnlineDm = false
  var dmOnlineHandlers: List[com.mobikok.ssp.data.streaming.handler.dm.online.Handler] = _
  try {
    isEnableOnlineDm = config.getBoolean(s"modules.$moduleName.dm.online.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableOnlineDm) {
    try {
      dmOnlineHandlers = config.getConfigList(s"modules.$moduleName.dm.online.handlers").map{ handlerConfig =>
        val h = Class.forName(handlerConfig.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.online.Handler]
        h.init(moduleName, mixTransactionManager, clickHouseClient, rDBConfig, kafkaClient, messageClient, kylinClient, hbaseClient, hiveContext, handlerConfig, config)
        h
      }.toList
    } catch {
      case _: Exception =>
    }
  }
  //-------------------------  Dm End  -------------------------


  def initDwiHandlers(): Unit = {
    val uuidFilterHandler = new UUIDFilterDwiHandler(uuidFilter, businessTimeExtractBy, isEnableDwiUuid)
    uuidFilterHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, null, config, config, "null", Array[String]())

    if (config.hasPath(s"modules.$moduleName.dwi.handler")) {
      dwiHandlers = config.getConfigList(s"modules.$moduleName.dwi.handler").map { x =>
        val hc = x
        val className = hc.getString("class")
        if (className.contains(".core.")) {
          throw new IllegalArgumentException("Core handler can not be set in the handler configuration")
        }
        val h = Class.forName(className).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
        h.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, hc, config, null, null)
        LOG.warn("init dwi handler", h.getClass.getName)
        h
      }.toList
    }
    if (dwiHandlers == null) {
      dwiHandlers = List()
    }
    if (isEnableDwi) {
      val hiveDWIPersistHandler = new HiveDWIPersistHandler()
      hiveDWIPersistHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, config, config, null, null)
      dwiHandlers = dwiHandlers :+ hiveDWIPersistHandler
    }
    if (dwiPhoenixEnable) {
      val hbaseDWIPersistHandler = new HBaseDWIPersistHandler()
      hbaseDWIPersistHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, config, config, null, null)
      dwiHandlers = dwiHandlers :+ hbaseDWIPersistHandler
    }
    dwiHandlers = uuidFilterHandler :: dwiHandlers

  }

  @volatile var hiveCleanable, clickHouseCleanable, hbaseCleanable, kafkaCleanable, phoenixCleanable: Cleanable = _

  override def init(): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} init started", moduleName)
      //      AS3.main3(sqlContext)
      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      hiveClient.init()
      if (hbaseClient != null) hbaseClient.init()
      kafkaClient.init()
      phoenixClient.init()
//      mysqlClient.init()
      clickHouseClient.init()

      initDwiHandlers()

      hiveCleanable = hiveClient.rollback()
//      mysqlCleanable = mysqlClient.rollback()
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
           |    "${argsConfig.getElse(ArgsConfig.REBRUSH, "NA").toUpperCase}",
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
          .getLastOffset(offsetRanges.map { x => x.topic })
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

          dwi = dwi.alias("dwi").persist(StorageLevel.MEMORY_ONLY_SER)
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

//                var asyncTaskCount = 0
                // 记录所有异步handler的数量
                val asyncHandlers = new util.ArrayList[Handler]()
                if (dwiHandlers != null) {
                  LOG.warn(s"""dwi handlers:\n${dwiHandlers.map{ h => h.getClass.getName}.mkString("\n")}""")
                  asyncHandlers.addAll(dwiHandlers.filter{ h => h.isAsynchronous })
                }
                if (dwrHandlers != null && dwrHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                  }
                  LOG.warn(s"""dwr handlers:\n${dwrHandlers.map{ h => h.getClass.getName}.mkString("\n")}""")
                  asyncHandlers.addAll(dwrHandlers.filter{ h => h.isAsynchronous })
                }
                if (dmOnlineHandlers != null && dmOnlineHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dm.online.handler' must config: master=true")
                  }
                  LOG.warn(s"""dm online handlers:\n${dmOnlineHandlers.map{ h => h.getClass.getName}.mkString("\n")}""")
                  asyncHandlers.addAll(dmOnlineHandlers.filter{ h => h.isAsynchronous })
                }
                if (dmOfflineHandlers != null && dmOfflineHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dm.online.handler' must config: master=true")
                  }
                  LOG.warn(s"""dm offline handlers:\n${dmOfflineHandlers.map{ h => h.getClass.getName}.mkString("\n")}""")
                  asyncHandlers.addAll(dmOfflineHandlers.filter{ h => h.isAsynchronous })
                }
                LOG.warn(s"""asyncHandlers:\n ${asyncHandlers.map{ h => h.getClass.getName }.mkString("\n")}""")
                // 全局计数，记录异步执行的handler数量，异步执行完后再执行commit操作
                val countDownLatch = new CountDownLatch(asyncHandlers.size())

//                val transactionCookies = new util.ArrayList[(String, TransactionCookie)]()
                //-----------------------------------------------------------------------------------------------------------------
                //  Begin Transaction !!
                //-----------------------------------------------------------------------------------------------------------------
                val parentTid = mixTransactionManager.beginTransaction(moduleName, groupName)

                val dwiLTimeExpr = s"'${mixTransactionManager.dwiLoadTime()}'"
                val dwrLTimeExpr = s"'${mixTransactionManager.dwrLoadTime()}'"
                //-----------------------------------------------------------------------------------------------------------------
                //  DWI Handler
                //-----------------------------------------------------------------------------------------------------------------
                var handledDwi: DataFrame = dwi
                if (dwiHandlers.nonEmpty) {
                  dwiHandlers.filter{ h => !h.isAsynchronous }.foreach{ h =>
                    val (middleDwi, _) = h.handle(handledDwi)
                    handledDwi = middleDwi
                    moduleTracer.trace(s"dwi ${h.getClass.getSimpleName} handle")
                  }
                }
                if (handledDwi != dwi) {
                  handledDwi.persist(StorageLevel.MEMORY_ONLY_SER)
//                  handledDwi.count()
                }

                //-----------------------------------------------------------------------------------------------------------------
                //  DWR prepare handle
                //-----------------------------------------------------------------------------------------------------------------
                var dwrDwi = handledDwi
                if (isEnableDwr && dwrHandlers != null && dwrHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                  }
                  dwrHandlers.filter( h => !h.isAsynchronous).foreach { h =>
                    dwrDwi = h.prepare(dwrDwi)
                    moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} filter")
                  }
                }

                var preparedDwr = dwrDwi

                if (isEnableDwr) {
                  preparedDwr = dwrDwi
                    .withColumn("l_time", expr(dwrLTimeExpr))
                    .withColumn("b_date", to_date(expr(businessTimeExtractBy)).cast("string"))
                    .withColumn("b_time", expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwrBTimeFormat')").as("b_time"))
                    .groupBy(col("l_time") :: col("b_date") :: col("b_time") :: dwrGroupByExprs: _*)
                    .agg(aggExprs.head, aggExprs.tail: _*)
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Wait union all module dwr data
                //-----------------------------------------------------------------------------------------------------------------
                moduleTracer.trace("wait dwr union all", {
                  mixModulesBatchController.waitUnionAll(preparedDwr, isMaster, moduleName)
                })


                //-----------------------------------------------------------------------------------------------------------------
                //  DWR handle
                //-----------------------------------------------------------------------------------------------------------------
                if (isEnableDwr && dwrHandlers != null && dwrHandlers.nonEmpty) {
                  if (!isMaster) {
                    LOG.warn(dwrHandlers.head.getClass.getName)
                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                  }
                  dwrHandlers.filter{ h => !h.isAsynchronous }.foreach{ h =>
                    mixModulesBatchController.set({
                      h.handle(mixModulesBatchController.get())
                    })
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                //  DM online handle
                //-----------------------------------------------------------------------------------------------------------------
                if (isEnableOnlineDm && dmOnlineHandlers != null && dmOnlineHandlers.nonEmpty) {
                  if (!isMaster) {
                    LOG.warn(s"Error handler: ${dmOnlineHandlers.head.getClass.getName}")
                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                  }
                  dmOnlineHandlers.filter{ h => !h.isAsynchronous}.foreach{ h =>
                    moduleTracer.trace(s"dm online handler ${h.getClass.getSimpleName} start handling")
                    h.handle(mixModulesBatchController.get())
                    moduleTracer.trace(s"dm online handler ${h.getClass.getSimpleName} start handling")
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                //  DM offline handle
                //-----------------------------------------------------------------------------------------------------------------
                if (isEnableOfflineDm && dmOfflineHandlers != null && dmOfflineHandlers.nonEmpty) {
                  if (!isMaster) {
                    LOG.warn(s"Error handler: ${dmOfflineHandlers.head.getClass.getName}")
                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
                  }
                  dmOfflineHandlers.filter{ h => !h.isAsynchronous }.foreach{ h =>
                    moduleTracer.trace(s"dm offline handler ${h.getClass.getSimpleName} start handling")
                    h.handle()
                    moduleTracer.trace(s"dm offline handler ${h.getClass.getSimpleName} finish handling")
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Asynchronous handle
                //-----------------------------------------------------------------------------------------------------------------
                if (asyncHandlers.nonEmpty) {
                  if (isMaster) {
                    mixModulesBatchController.get().cache()
                    mixModulesBatchController.get().count()
                  }
                  asyncHandlers.foreach {

                    case dwiHandler: com.mobikok.ssp.data.streaming.handler.dwi.Handler =>
                      ThreadPool.execute {
                        LOG.warn(s"dwi handle, className=${dwiHandler.getClass.getName}")
                        try {
                          dwiHandler.handle(handledDwi)
                          countDownLatch.synchronized {
                            countDownLatch.countDown()
                          }
                        } catch {
                          case e: Exception => LOG.warn(ExceptionUtil.getStackTraceMessage(e))
                        }
                      }
                    case dwrHandler: com.mobikok.ssp.data.streaming.handler.dwr.Handler =>
                      ThreadPool.execute {
                        LOG.warn(s"dwr handle, className=${dwrHandler.getClass.getName}")
                        try {
                          dwrHandler.handle(mixModulesBatchController.get())
                          countDownLatch.synchronized {
                            countDownLatch.countDown()
                          }
                        } catch {
                          case e: Exception => LOG.warn(ExceptionUtil.getStackTraceMessage(e))
                        }
                      }
                    case dmOnlineHandler: com.mobikok.ssp.data.streaming.handler.dm.online.Handler =>
                      ThreadPool.execute {
                        LOG.warn(s"dmOnline handle, className=${dmOnlineHandler.getClass.getName}")
                        try {
                          dmOnlineHandler.handle(mixModulesBatchController.get())
                          countDownLatch.synchronized {
                            countDownLatch.countDown()
                          }
                        } catch {
                          case e: Exception => LOG.warn(ExceptionUtil.getStackTraceMessage(e))
                        }
                      }
                    case dmOfflineHandler: com.mobikok.ssp.data.streaming.handler.dm.offline.Handler =>
                      ThreadPool.execute {
                        LOG.warn(s"dmOffline handle, className=${dmOfflineHandler.getClass.getName}")
                        try {
                          dmOfflineHandler.handle()
                          countDownLatch.synchronized {
                            countDownLatch.countDown()
                          }
                        } catch {
                          case e: Exception => LOG.warn(ExceptionUtil.getStackTraceMessage(e))
                        }
                      }
                    case _ =>
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Kafka set offset
                //-----------------------------------------------------------------------------------------------------------------
                var kafkaT: TransactionCookie = null
                kafkaT = kafkaClient.setOffset(parentTid, offsetRanges)
                LOG.warn("kafkaClient.setOffset completed", kafkaT)
                //每半个小时保存一次offset消息 便于异常回滚之前某个时刻的offset
                val updateTime = CSTTime.now.modifyMinuteAsTime(-30)
                val KEEP_OFFSET_CER = s"${moduleName}_keep_offset_cer"
                val KEEP_OFFSET_TOPIC = s"${moduleName}_keep_offset_topic"
                val latestPartitionOffsetTopic = s"${moduleName}_partition_offset_topic"

                MC.pull(KEEP_OFFSET_CER, Array(KEEP_OFFSET_TOPIC), { x =>

                  val lastTime = if (x.nonEmpty) x.reverse.map(_.getKeyBody).take(1).head else ""

                  val needUpdate = x.isEmpty || (CSTTime.date2TimeStamp(lastTime) <= CSTTime.date2TimeStamp(updateTime))

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

                cacheTransactionCookies(COOKIE_KIND_KAFKA_T, kafkaT)

                //------------------------------------------------------------------------------------
                // Commit Transaction
                //------------------------------------------------------------------------------------


                dwiHandlers.filter{ x => !x.isAsynchronous }.foreach{ h => h.commit(null) }

                if (dwrHandlerCookies != null) {
                  dwrHandlers.filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous }
                    .foreach{ h => h.asInstanceOf[Transactional].commit(null) }
                }

                if (dmOnlineHandlers != null) {
                  dmOnlineHandlers.filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous}
                    .foreach{ h => h.asInstanceOf[Transactional].commit(null)}
                }

                if (dmOfflineHandlers != null) {
                  dmOfflineHandlers.filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous}
                    .foreach{ h => h.asInstanceOf[Transactional].commit(null)}
                }

                // asynchronous handler await before commit
                LOG.warn("wait all handlers commit")

                startCountDownHeartbeats(countDownLatch)
                countDownLatch.await()
                LOG.warn("handlers commit finished!")

                asyncHandlers.filter{ h => h.isInstanceOf[Transactional] }
                  .par.foreach{ h => h.asInstanceOf[Transactional].commit(null) }

                //------------------------------------------------------------------------------------
                // Wait all module transaction commit
                //------------------------------------------------------------------------------------
                LOG.warn(s"wait transaction commit start", "tid", parentTid, "master", isMaster)
                // Key code !!
                moduleTracer.trace("wait master tx commit", {
                  mixTransactionManager.waitAllModuleReadyCommit(isMaster, moduleName, {
                    mixTransactionManager.commitTransaction(isMaster, parentTid, moduleName)
                    mixModulesBatchController.completeBatch(isMaster)
                  })
                })
                LOG.warn(s"wait transaction commit done")

                //------------------------------------------------------------------------------------
                // Clean
                //------------------------------------------------------------------------------------
                if (dwi != null) dwi.unpersist()
                if (handledDwi != null) handledDwi.unpersist()
                // 清理上一次启动的产生的事务数据
                if (!mixTransactionManager.needTransactionalAction()) {
                  if (hiveCleanable != null) {
                    hiveCleanable.doActions()
                    hiveCleanable = null
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

                dwiHandlers.foreach{ h => h.clean() }
                if (isMaster) {
                  if (dwrHandlers != null) {
                    dwrHandlers.filter{h => h.isInstanceOf[Transactional]}.par.foreach{h => h.asInstanceOf[Transactional].clean()}
                  }
                  if (dmOnlineHandlers != null) {
                    dmOnlineHandlers.filter{ h => h.isInstanceOf[Transactional]}.par.foreach{h => h.asInstanceOf[Transactional].clean()}
                  }
                  if (dmOfflineHandlers != null) {
                    dmOfflineHandlers.filter{ h => h.isInstanceOf[Transactional]}.par.foreach{h => h.asInstanceOf[Transactional].clean()}
                  }
                }

                kafkaClient.clean(popNeedCleanTransactions(COOKIE_KIND_KAFKA_T, parentTid): _*)

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
                     |    "${argsConfig.getElse(ArgsConfig.REBRUSH, "NA").toUpperCase}",
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
    val pr = batchsTransactionCookiesCache.get(cookieKind)
    val isTran = mixTransactionManager.needTransactionalAction()
    if (pr != null && isTran) {
      val needCleans = pr.filter(!_.parentId.equals(excludeCurrTransactionParentId))
      pr.removeAll(needCleans)
      result = needCleans.toArray
    }
    result
  }

  private def pushChangedHivePartition(dwrT: HiveTransactionCookie, dwiT: HiveTransactionCookie): Unit = {

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
      val key = OM.toJOSN(dwrT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")
    } else {
      LOG.warn(s"MessageClient dwr no hive partitions to push", s"topic: $topic")
    }

  }

  def startCountDownHeartbeats(countDownLatch: CountDownLatch): Unit = {

    ThreadPool.execute {
      var b = true
      while (b) {
        if (countDownLatch.getCount > 0) {
          countDownLatch.synchronized {
            LOG.warn(s"$moduleName left task count are: ${countDownLatch.getCount}")
          }
        } else {
          b = false
        }
        Thread.sleep(1000 * 60)
      }
    }
  }

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
      case _: Exception =>
    }
  }

  def runInTry(func: => Unit, catchFunc: => Unit): Unit = {
    try {
      func
    } catch {
      case _: Exception =>
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
