package com.mobikok.ssp.data.streaming.module

import java.text.DecimalFormat
import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.mobikok.message.{Message, MessagePushReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.MonitorClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2
import com.mobikok.ssp.data.streaming.config.ArgsConfig.Value
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{LatestOffsetRecord, OffsetRange}
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.Handler
import com.mobikok.ssp.data.streaming.handler.dm.offline.{ClickHouseQueryByBDateHandler, ClickHouseQueryByBTimeHandler, ClickHouseQueryMonthHandler}
import com.mobikok.ssp.data.streaming.handler.dwi.core.{HBaseDWIPersistHandler, HiveDWIPersistHandler, UUIDFilterDwiHandler}
import com.mobikok.ssp.data.streaming.handler.dwr.core._
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

class PluggableModule(globalConfig: Config,
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
  val ksc = Class.forName(globalConfig.getString(s"modules.$moduleName.dwi.kafka.schema"))
  val dwiStructType = ksc.getMethod("structType").invoke(ksc.newInstance()).asInstanceOf[StructType]
  var moduleConfig = globalConfig.getConfig(s"modules.$moduleName")
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

  val shufflePartitions = globalConfig.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  var aggExprs: List[Column] = _
  try {
    aggExprs = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var aggExprsAlias: List[String] = _
  try {
    aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
  } catch {
    case _: Exception =>
  }

  var unionAggExprsAndAlias: List[Column] = _
  try {
    unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var businessTimeExtractBy: String = _
  try {
    businessTimeExtractBy = globalConfig.getString(s"modules.$moduleName.business.time.extract.by")
  } catch {
    case _: Throwable =>
      //兼容历史代码
      try{
        businessTimeExtractBy = globalConfig.getString(s"modules.$moduleName.business.date.extract.by")
      }catch {case e:Throwable=>
        businessTimeExtractBy = globalConfig.getString(s"modules.$moduleName.b_time.input")
      }
  }

  var isFastPollingEnable = false
  if (globalConfig.hasPath(s"modules.$moduleName.fast.polling.enable")) {
    isFastPollingEnable = globalConfig.getBoolean(s"modules.$moduleName.fast.polling.enable")
  }

  val monitorClient = new MonitorClient(messageClient)
  val topics = globalConfig.getConfigList(s"modules.$moduleName.kafka.consumer.partitoins").map { x => x.getString("topic") }.toArray[String]

  var isExcludeOfflineRebrushPart = false
  if (ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))) {
    isExcludeOfflineRebrushPart = true
  }

  var kafkaProtoEnable = false
  var kafkaProtoClass: Class[_] = _
  try {
    kafkaProtoEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.kafka.proto.enable")
    kafkaProtoClass = Class.forName(globalConfig.getString(s"modules.$moduleName.dwi.kafka.proto.class"))
  } catch {
    case _: Exception =>
  }

  val isMaster = mixModulesBatchController.isMaster(moduleName)

  //-------------------------  Constants And Fields End  -------------------------


  //-------------------------  Tools  -------------------------
  //Value Type: String or Array[Byte]
  var stream: InputDStream[ConsumerRecord[String, Object]] = _
  var moduleTracer: ModuleTracer = new ModuleTracer(moduleName, globalConfig, mixModulesBatchController)
  //  val transactionManager: TransactionManager = new TransactionManager(config)
  val mixTransactionManager = mixModulesBatchController.getMixTransactionManager()

  //-------------------------  Tools End  -------------------------


  //-------------------------  Clients  -------------------------
  val hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
  val sqlContext = SQLContextGenerater.generate(ssc.sparkContext)
  var kylinClient: KylinClientV2 = _
  try {
    kylinClient = new KylinClientV2(globalConfig.getString("kylin.client.url"))
  } catch {
    case e: Exception => LOG.warn("KylinClient init fail !!", e.getMessage)
  }

  var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    moduleName, globalConfig.getString(s"rdb.url"), globalConfig.getString(s"rdb.user"), globalConfig.getString(s"rdb.password")
  )

  var bigQueryClient = null.asInstanceOf[BigQueryClient]
  try {
    bigQueryClient = new BigQueryClient(moduleName, globalConfig, ssc, messageClient, hiveContext)
  } catch {
    case e: Throwable => LOG.warn("BigQueryClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")
  }

  val rDBConfig = new RDBConfig(mySqlJDBCClientV2)

  val messageClient = new MessageClient(moduleName, globalConfig.getString("message.client.url"))
  var hbaseClient = new HBaseMultiSubTableClient(moduleName, ssc.sparkContext, globalConfig, mixTransactionManager, moduleTracer)
  val hiveClient = new HiveClient(moduleName, globalConfig, ssc, messageClient, mixTransactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, globalConfig, mixTransactionManager)
  val phoenixClient = new PhoenixClient(moduleName, ssc.sparkContext, globalConfig, mixTransactionManager, moduleTracer)
  //  val greenplumClient = new GreenplumClient(moduleName, config, ssc, messageClient, mixTransactionManager, moduleTracer)
  val greenplumClient = null.asInstanceOf[GreenplumClient]

//  var h2JDBCClient = null.asInstanceOf[H2JDBCClient]
//  try {
//    h2JDBCClient = new H2JDBCClient(h2JDBCConnectionUrl, "", "")
//  } catch {
//    case e: Throwable => LOG.warn("H2JDBCClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")
//  }

//  val mysqlClient = new MySQLClient(moduleName, ssc.sparkContext, config, messageClient, mixTransactionManager, moduleTracer)
  val clickHouseClient = new ClickHouseClient(moduleName, globalConfig, ssc, messageClient, mixTransactionManager, hiveContext, moduleTracer)
  //-------------------------  Clients End  -------------------------


  //-------------------------  Dwi about  -------------------------
  var dwiPhoenixEnable = false
  var dwiPhoenixSubtableEnable = true
  var dwiPhoenixTable = null.asInstanceOf[String]
  var dwiPhoenixHBaseStorableClass = null.asInstanceOf[String]
  try {
    dwiPhoenixEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.enable")
  } catch {
    case _: Exception =>
  }
  if (dwiPhoenixEnable) {
    dwiPhoenixTable = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.table")
    dwiPhoenixHBaseStorableClass = globalConfig.getString(s"modules.$moduleName.dwi.phoenix.hbase.storable.class")
  }
  if (globalConfig.hasPath(s"modules.$moduleName.dwi.phoenix.subtable.enable")) {
    dwiPhoenixSubtableEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.subtable.enable")
  }

  var isEnableDwi = false
  var dwiTable: String = _
  try {
    isEnableDwi = globalConfig.getBoolean(s"modules.$moduleName.dwi.enable")
  } catch {
    case _: Exception =>
  }
  try {
    dwiTable = globalConfig.getString(s"modules.$moduleName.dwi.table")
  } catch {
    case e: Throwable =>
      if (isEnableDwi) throw e
  }

//  var dwiHandlerCookies: Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])] = _ // Array[(com.mobikok.ssp.data.streaming.handler.dwi.Handler, Array[TransactionCookie])]()
  var dwiHandlers: List[com.mobikok.ssp.data.streaming.handler.dwi.Handler] = _
  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwiBTimeFormat = globalConfig.getString(s"modules.$moduleName.dwi.business.time.format.by")
  } catch {
    case _: Throwable =>
  }

  var isEnableDwiUuid = false
  var dwiUuidStatHbaseTable: String = _
  var dwiUuidFieldsAlias: String = "rowkey"
  var dwiUuidFields: Array[String] = _
  try {
    isEnableDwiUuid = globalConfig.getBoolean(s"modules.$moduleName.dwi.uuid.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableDwiUuid) {
    dwiUuidFields = globalConfig.getStringList(s"modules.$moduleName.dwi.uuid.fields").toArray[String](new Array[String](0))
    try {
      dwiUuidFieldsAlias = globalConfig.getString(s"modules.$moduleName.dwi.uuid.alias")
    } catch {
      case _: Exception =>
    }

    dwiUuidStatHbaseTable = dwiTable + "_uuid"
    try {
      dwiUuidStatHbaseTable = globalConfig.getString(s"modules.$moduleName.dwi.uuid.stat.hbase.table")
    } catch {
      case _: Exception =>
    }
  }

  var uuidFilter: UuidFilter = _
  if (globalConfig.hasPath(s"modules.$moduleName.uuid.filter.class")) {
    val f = globalConfig.getString(s"modules.$moduleName.uuid.filter.class")
    uuidFilter = Class.forName(f).newInstance().asInstanceOf[UuidFilter]
  } else {
    uuidFilter = new DefaultUuidFilter(dwiBTimeFormat)
  }
  uuidFilter.init(moduleName, globalConfig, hiveContext, moduleTracer, dwiUuidFieldsAlias, businessTimeExtractBy, dwiTable)
  //-------------------------  Dwi End  -------------------------


  //-------------------------  Dwr about  -------------------------

  var isEnableDwr = false
  var dwrTable: String = _
  try {
    isEnableDwr = globalConfig.getBoolean(s"modules.$moduleName.dwr.enable")
  } catch {case _: Exception =>}
  if (isEnableDwr) {
    dwrTable = globalConfig.getString(s"modules.$moduleName.dwr.table")
  }

  var dwrGroupByExprs: List[Column] = _
  try {
    dwrGroupByExprs = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var dwrGroupByExprsAlias: Array[String] = _
  try {
    dwrGroupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray[String]
  } catch {
    case _: Exception =>
  }

  var dwrGroupByExprsAliasCol: List[Column] = _
  try {
    dwrGroupByExprsAliasCol = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => col(x.getString("as"))
    }.toList
  } catch {
    case _: Exception =>
  }

  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwrBTimeFormat = globalConfig.getString(s"modules.$moduleName.dwr.business.time.format.by")
  } catch {
    case _: Throwable =>
  }

  var dwrIncludeRepeated = true
  if (globalConfig.hasPath(s"modules.$moduleName.dwr.include.repeated")) {
    dwrIncludeRepeated = globalConfig.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  } else if (isEnableDwiUuid) {
    dwrIncludeRepeated = false
  }

  var dimCounterMaps:Map[String, String] = _
  try {
    dimCounterMaps = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => if(x.hasPath("map")) x.getString("as") -> x.getString("map") else null
    }.filter(_ != null).toMap
  } catch {
    case e: Throwable =>
  }

  var dwrHandlers: util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = _
  var dwrHandlerCookies: Array[(String, TransactionCookie)] = _

  //-------------------------  Dwr End  -------------------------

  //-------------------------  Dm about  -------------------------
  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(globalConfig.getString(s"modules.$moduleName.dm.load.time.format.by"))
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
    isEnableOfflineDm = globalConfig.getBoolean(s"modules.$moduleName.dm.offline.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableOfflineDm) {
    try {
      dmOfflineHandlers = globalConfig.getConfigList(s"modules.$moduleName.dm.offline.handlers").map { setting =>
        val h = Class.forName(setting.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.offline.Handler]
        h.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClient, hbaseClient, hiveContext, argsConfig, setting)
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
    isEnableOnlineDm = globalConfig.getBoolean(s"modules.$moduleName.dm.online.handler.enable")
  } catch {
    case _: Exception =>
  }
  if (isEnableOnlineDm) {
    try {
      dmOnlineHandlers = globalConfig.getConfigList(s"modules.$moduleName.dm.online.handlers").map{ handlerConfig =>
        val h = Class.forName(handlerConfig.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dm.online.Handler]
        h.init(moduleName, mixTransactionManager, clickHouseClient, rDBConfig, kafkaClient, messageClient, kylinClient, hbaseClient, hiveContext, handlerConfig, globalConfig)
        h
      }.toList
    } catch {
      case _: Exception =>
    }
  }
  //-------------------------  Dm End  -------------------------


  def initDwiHandlers(): Unit = {
    val uuidFilterHandler = new UUIDFilterDwiHandler(uuidFilter, businessTimeExtractBy, isEnableDwiUuid, dwiBTimeFormat, argsConfig)
    uuidFilterHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, "null", Array[String]())

    if (globalConfig.hasPath(s"modules.$moduleName.dwi.handler")) {
      dwiHandlers = globalConfig.getConfigList(s"modules.$moduleName.dwi.handler").map { x =>
        val hc = x
        val className = hc.getString("class")
        if (className.contains(".core.")) {
          throw new IllegalArgumentException("Core handler can not be set in the handler configuration")
        }
        val h = Class.forName(className).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
        h.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, hc, globalConfig, null, null)
        LOG.warn("init dwi handler", h.getClass.getName)
        h
      }.toList
    }
    if (dwiHandlers == null) {
      dwiHandlers = List()
    }
    if (isEnableDwi) {
      val hiveDWIPersistHandler = new HiveDWIPersistHandler()
      hiveDWIPersistHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, null, null)
      dwiHandlers = dwiHandlers :+ hiveDWIPersistHandler
    }
    if (dwiPhoenixEnable) {
      val hbaseDWIPersistHandler = new HBaseDWIPersistHandler()
      hbaseDWIPersistHandler.init(moduleName, mixTransactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, null, null)
      dwiHandlers = dwiHandlers :+ hbaseDWIPersistHandler
    }
    dwiHandlers = uuidFilterHandler :: dwiHandlers

  }

  def initDwrHandlers(): Unit ={
//    if (isMaster) {
      dwrHandlers = new util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler]()
      if (!dwrIncludeRepeated) {
        dwrHandlers.add(new UUIDFilterDwrHandler(uuidFilter))
      }
      // 核心handler的手动配置
      if (isEnableDwr) {

        // 小众数据单维度统计
        if(dimCounterMaps != null && dimCounterMaps.nonEmpty) {
          val counterHandler = new HiveCounterHandler()
          counterHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, "", "")
          dwrHandlers.add(counterHandler)
        }

        val dwrPersistHandler = new HiveDWRPersistHandlerV2()
        dwrPersistHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, "", "")
        dwrHandlers.add(dwrPersistHandler)
      }

      var enableDwrAccDay = false
      runInTry{enableDwrAccDay = globalConfig.getBoolean(s"modules.$moduleName.dwr.acc.day.enable")}
      if (enableDwrAccDay) {
        val dwrPersistDayHandler = new HiveDWRPersistDayHandler()
        dwrPersistDayHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, "", "")
        dwrHandlers.add(dwrPersistDayHandler)
      }

      var enableDwrAccMonth = false
      runInTry{enableDwrAccMonth = globalConfig.getBoolean(s"modules.$moduleName.dwr.acc.month.enable")}
      if (enableDwrAccMonth) {
        val dwrPersistMonthHandler = new HiveDWRPersistMonthHandler()
        dwrPersistMonthHandler.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, "", "")
        dwrHandlers.add(dwrPersistMonthHandler)
      }
      try {
        globalConfig.getConfigList(s"modules.$moduleName.dwr.handler").foreach { x =>
          val hc = x
          val h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
          val expr = if (x.hasPath("expr")) x.getString("expr") else ""
          val as = if (x.hasPath("as")) x.getString("as") else ""
          h.init(moduleName, mixTransactionManager, hbaseClient, hiveClient, clickHouseClient, hc, globalConfig, expr, as)
          dwrHandlers.add(h)
        }
      } catch {
        case e: Exception => LOG.warn(s"Dwr init error, exception: ${e.getMessage}")
      }
//    }
  }

  @volatile var hiveCleanable, clickHouseCleanable, hbaseCleanable, kafkaCleanable, phoenixCleanable: Cleanable = _

  override def init(): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} init started", moduleName)
      //      AS3.main3(sqlContext)
//      GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

      hiveClient.init()
      if (hbaseClient != null) hbaseClient.init()
      kafkaClient.init()
      phoenixClient.init()
//      mysqlClient.init()
      clickHouseClient.init()

      tryCreateTableForVersionFeatures()

      initDwiHandlers()
      initDwrHandlers()

      val rollback = argsConfig.get(ArgsConfig.ROLLBACK, Value.ROLLBACK_TRUE)
      if(Value.ROLLBACK_TRUE.equals(rollback)) {
        hiveCleanable = hiveClient.rollback()
  //      mysqlCleanable = mysqlClient.rollback()
        if (hbaseClient != null) hbaseCleanable = hbaseClient.rollback()
        kafkaCleanable = kafkaClient.rollback()
        phoenixCleanable = phoenixClient.rollback()
        clickHouseCleanable = clickHouseClient.rollback()
      }else {
        LOG.warn("ArgsConfig parameter configuration do not rollback ")
      }

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
           |    ${(100.0 * globalConfig.getInt("spark.conf.streaming.batch.buration") / 60).asInstanceOf[Int] / 100.0},
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

      setCommitedOffsetIfSpecified()

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

      if (globalConfig.hasPath(dwiTPath) && globalConfig.getBoolean(dwiEnable)) {
        val dwiT = globalConfig.getString(dwiTPath)
        hiveContext.sql(s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwi Table", s"create table if not exists $dwiT like ${dwiT.split(clonePrefix)(1)}")

      }

      if (globalConfig.hasPath(dwrTPath) && globalConfig.getBoolean(dwrEnable) && globalConfig.hasPath(master) && globalConfig.getBoolean(master)) {
        val dwrT = globalConfig.getString(dwrTPath)
        hiveContext.sql(s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")
        LOG.warn("cloneTables dwr Table", s"create table if not exists $dwrT like ${dwrT.split(clonePrefix)(1)}")

      }

      if (globalConfig.hasPath(hbTPath) && globalConfig.getBoolean(hbEnable)) {
        val hbT = globalConfig.getString(hbTPath)
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

    stream = kafkaClient.createDirectStream(globalConfig, ssc, kafkaClient.getCommitedOffset(topics: _*), moduleName)

    stream.foreachRDD { source =>
      try {

        val order = mixTransactionManager.generateTransactionOrder(moduleName)
        moduleTracer.startBatch(order, "INIT")

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
            ProtobufUtil.protobufToJSON(_kafkaProtoClass.asInstanceOf[Class[com.google.protobuf.Message]], x.value().asInstanceOf[Array[Byte]])
          }
        } else {
          jsonSource = source.map(_.value().asInstanceOf[String])
        }

        val filtered = jsonSource

        // 待改
        if (false /*isFastPollingEnable  && filtered.isEmpty() && GlobalAppRunningStatusV2.isPreviousRunning(concurrentGroup, moduleName)*/) {
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
//          moduleTracer.trace("wait module queue", {
//            GlobalAppRunningStatusV2.waitRunAndSetRunningStatus(concurrentGroup, moduleName)
//          })

          val groupName = if (StringUtil.notEmpty(dwrTable)) dwrTable else moduleName

          //-----------------------------------------------------------------------------------------------------------------
          //  Begin Transaction !!
          //-----------------------------------------------------------------------------------------------------------------
          val parentTid = mixTransactionManager.beginTransaction(moduleName, groupName, order)

          // 开始异步处理
//          new Thread(new Runnable {
//
//            override def run(): Unit = {

              try {

                moduleTracer.startBatch(order, parentTid)

//                var asyncTaskCount = 0
                // 记录所有异步handler的数量
                val asyncHandlers = new util.ArrayList[Handler]()
                if (dwiHandlers != null) {
                  LOG.warn("dwi handlers", dwiHandlers.map{ h => h.getClass.getName})
                  asyncHandlers.addAll(dwiHandlers.filter{ h => h.isAsynchronous })
                }
                if (dwrHandlers != null && dwrHandlers.nonEmpty) {
//                  if (!isMaster) {
//                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
//                  }
                  LOG.warn("dwr handlers", dwrHandlers.map{ h => h.getClass.getName})
                  asyncHandlers.addAll(dwrHandlers.filter{ h => h.isAsynchronous })
                }
                if (dmOnlineHandlers != null && dmOnlineHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dm.online.handler' must config: master=true")
                  }
                  LOG.warn("dm online handlers", dmOnlineHandlers.map{ h => h.getClass.getName})
                  asyncHandlers.addAll(dmOnlineHandlers.filter{ h => h.isAsynchronous })
                }
                if (dmOfflineHandlers != null && dmOfflineHandlers.nonEmpty) {
                  if (!isMaster) {
                    throw new ModuleException("Module of include 'dm.online.handler' must config: master=true")
                  }
                  LOG.warn("dm offline handlers", dmOfflineHandlers.map{ h => h.getClass.getName})
                  asyncHandlers.addAll(dmOfflineHandlers.filter{ h => h.isAsynchronous })
                }
                LOG.warn("async handlers", asyncHandlers.map{ h => h.getClass.getName })
                // 全局计数，记录异步执行的handler数量，异步执行完后再执行commit操作
//                val asyncHandlersCountDownLatch = new CountDownLatch(asyncHandlers.size())
                val asyncWorker = new AsyncWorker(moduleName, asyncHandlers.size())

//                val transactionCookies = new util.ArrayList[(String, TransactionCookie)]()
//                //-----------------------------------------------------------------------------------------------------------------
//                //  Begin Transaction !!
//                //-----------------------------------------------------------------------------------------------------------------
//                val parentTid = mixTransactionManager.beginTransaction(moduleName, groupName, true)

                val dwiLTimeExpr = s"'${mixTransactionManager.dwiLoadTime(moduleConfig)}'"
                val dwrLTimeExpr = s"'${mixTransactionManager.dwrLoadTime(moduleConfig)}'"
                //-----------------------------------------------------------------------------------------------------------------
                //  DWI Handler
                //-----------------------------------------------------------------------------------------------------------------
                var handledDwi: DataFrame = dwi
                if (dwiHandlers.nonEmpty) {
                  dwiHandlers.filter{ h => !h.isAsynchronous }.foreach{ h =>
                    moduleTracer.trace(s"dwi ${h.getClass.getSimpleName} handle start")
                    val (middleDwi, _) = h.handle(handledDwi)
                    handledDwi = middleDwi
                    moduleTracer.trace(s"dwi ${h.getClass.getSimpleName} handle done")
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
//                  if (!isMaster) {
//                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
//                  }
                  // 同步和异步的handler预处理
                  dwrHandlers/*.filter( h => !h.isAsynchronous)*/.foreach { h =>
                    moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} prepare start")
                    dwrDwi = h.prepare(dwrDwi)
                    moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} prepare done")
                  }
                }

                var preparedDwr = dwrDwi

                if (isEnableDwr) {
                  preparedDwr = dwrDwi
                    .withColumn("l_time", expr(dwrLTimeExpr))
                    .withColumn("b_date", to_date(expr(businessTimeExtractBy)).cast("string"))
                    .withColumn("b_time", expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwrBTimeFormat')").as("b_time"))
                    .withColumn("b_version", expr("'0'")) // 默认0
                    .groupBy(col("l_time") :: col("b_date") :: col("b_time") :: col("b_version") :: dwrGroupByExprs: _*)
                    .agg(aggExprs.head, aggExprs.tail: _*)

                  //-----------------------------------------------------------------------------------------------------------------
                  // Wait union all module dwr data
                  //-----------------------------------------------------------------------------------------------------------------
                  moduleTracer.trace("wait dwr union all", {
                    mixModulesBatchController.waitUnionAll(preparedDwr, isMaster, moduleName)
                  })
                }

                //-----------------------------------------------------------------------------------------------------------------
                //  DWR handle
                //-----------------------------------------------------------------------------------------------------------------
                if (isEnableDwr && dwrHandlers != null && dwrHandlers.nonEmpty) {
//                  if (!isMaster) {
//                    LOG.warn(dwrHandlers.head.getClass.getName)
//                    throw new ModuleException("Module of include 'dwr.handler' must config: master=true")
//                  }
                  if(isMaster) {
                    dwrHandlers.filter{ h => !h.isAsynchronous }.foreach{ h =>
                      moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} handle start")
                      mixModulesBatchController.set({
                        h.handle(mixModulesBatchController.get())
                      })
                      moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} handle done")
                    }
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
                    moduleTracer.trace(s"dm online ${h.getClass.getSimpleName} handle start")
                    h.handle(mixModulesBatchController.get())
                    moduleTracer.trace(s"dm online ${h.getClass.getSimpleName} handle done")
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
                    moduleTracer.trace(s"dm offline ${h.getClass.getSimpleName} handle start")
                    h.handle()
                    moduleTracer.trace(s"dm offline ${h.getClass.getSimpleName} handler done")
                  }
                }

                //-----------------------------------------------------------------------------------------------------------------
                // Asynchronous handle
                //-----------------------------------------------------------------------------------------------------------------
                val pTheadId:java.lang.Long = Thread.currentThread().getId
                if (asyncHandlers.nonEmpty) {
//                  startAsyncHandlersCountDownHeartbeats(asyncHandlersCountDownLatch)

                  asyncHandlers.foreach {
                    case dwiHandler: com.mobikok.ssp.data.streaming.handler.dwi.Handler =>

                      asyncWorker.run {
                        val n = dwiHandler.getClass.getSimpleName
                        try {
                          moduleTracer.startBatch(order, parentTid,  pTheadId, "    ")

                          // handle
                          LOG.warn(s"dwi async ${n} handle start")
                          moduleTracer.trace(s"dwi async ${n} handle start")
                          dwiHandler.handle(handledDwi)
                          moduleTracer.trace(s"dwi async ${n} handle done")
                          LOG.warn(s"dwi async ${n} handle done")

                          // commit
                          LOG.warn(s"dwi async ${n} commit start")
                          moduleTracer.trace(s"dwi async ${n} commit start")
                          dwiHandler.commit(null)
                          moduleTracer.trace(s"dwi async ${n} commit done")
                          LOG.warn(s"dwi async ${n} commit done")

//                          asyncHandlersCountDownLatch.countDown()
                        } catch {
                          case e: Exception => {
                            LOG.warn("DWI asynchronous handle error", "handler", n, "exception",e)
                            throw e
                          }
                        }
                      }
                    case dwrHandler: com.mobikok.ssp.data.streaming.handler.dwr.Handler =>
                      asyncWorker.run {
                        if(isMaster) {
                          val n = dwrHandler.getClass.getSimpleName
                          try {
                            moduleTracer.startBatch(order, parentTid, pTheadId, "    ")

                            // handle
                            LOG.warn(s"dwr async ${n} handle start")
                            moduleTracer.trace(s"dwr async ${n} handle start")
                            dwrHandler.handle(mixModulesBatchController.get())
                            moduleTracer.trace(s"dwr async ${n} handle done")
                            LOG.warn(s"dwr async ${n} handle done")

                            // commit
                            if(dwrHandler.isInstanceOf[Transactional]){
                              LOG.warn(s"dwr async ${n} commit start")
                              moduleTracer.trace(s"dwr async ${n} commit start")
                              dwrHandler.asInstanceOf[Transactional].commit(null)
                              moduleTracer.trace(s"dwr async ${n} commit done")
                              LOG.warn(s"dwr async ${n} commit done")
                            }

  //                          asyncHandlersCountDownLatch.countDown()
                          } catch {
                            case e: Exception => {
                              LOG.warn("DWR asynchronous handle error", "handler", n, "exception", e)
                              throw e
                            }
                          }
                        }
                      }
                    case dmOnlineHandler: com.mobikok.ssp.data.streaming.handler.dm.online.Handler =>
                      asyncWorker.run {
                        if(isMaster) {
                          val n = dmOnlineHandler.getClass.getSimpleName
                          try {
                            moduleTracer.startBatch(order, parentTid, pTheadId, "    ")

                            // handle
                            LOG.warn(s"dm online async ${n} handle start")
                            moduleTracer.trace(s"dm online async ${n} handle start")
                            dmOnlineHandler.handle(mixModulesBatchController.get())
                            moduleTracer.trace(s"dm online async ${n} handle done")
                            LOG.warn(s"dm online async ${n} handle done")

                            // commit
                            if(dmOnlineHandler.isInstanceOf[Transactional]){
                              LOG.warn(s"dm online async ${n} commit start")
                              moduleTracer.trace(s"dm online async ${n} commit start")
                              dmOnlineHandler.asInstanceOf[Transactional].commit(null)
                              moduleTracer.trace(s"dm online async ${n} commit done")
                              LOG.warn(s"dm online async ${n} commit done")
                            }

  //                          asyncHandlersCountDownLatch.countDown()
                          } catch {
                            case e: Exception => {
                              LOG.warn(s"DM online asynchronous handle error", "handler", n, "exception", e)
                              throw e
                            }
                          }
                        }
                      }
                    case dmOfflineHandler: com.mobikok.ssp.data.streaming.handler.dm.offline.Handler =>
                      asyncWorker.run {
                        if(isMaster) {
                          val n = dmOfflineHandler.getClass.getSimpleName
                          try {
                            moduleTracer.startBatch(order, parentTid, pTheadId, "    ")

                            // handle
                            LOG.warn(s"dm offline async handle start", n)
                            moduleTracer.trace(s"dm offline async ${n} handle start")
                            dmOfflineHandler.handle()
                            moduleTracer.trace(s"dm offline async ${n} handle done")
                            LOG.warn(s"dm offline async handle done", n)

                            // commit
                            if(dmOfflineHandler.isInstanceOf[Transactional]){
                              LOG.warn(s"dm offline async ${n} commit start")
                              moduleTracer.trace(s"dm offline async ${n} commit start")
                              dmOfflineHandler.asInstanceOf[Transactional].commit(null)
                              moduleTracer.trace(s"dm offline ${n} commit done")
                              LOG.warn(s"dm offline async ${n} commit start")
                            }

  //                          asyncHandlersCountDownLatch.countDown()
                          } catch {
                            case e: Exception => {
                              LOG.warn("dm offline asynchronous handle error", "handler", n, "exception", e)
                              throw e
                            }
                          }
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
                // Synchronous handler commit transaction
                if(dwiHandlers != null) {
                  dwiHandlers
                    .filter{ x => !x.isAsynchronous }
                    .foreach{ h =>
                      moduleTracer.trace(s"dwi ${h.getClass.getSimpleName} commit start")
                      h.commit(null)
                      moduleTracer.trace(s"dwi ${h.getClass.getSimpleName} commit done")
                    }
                }
                if (isMaster && dwrHandlers != null) {
                  dwrHandlers
                    .filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous }
                    .foreach{ h =>
                      moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} commit start")
                      h.asInstanceOf[Transactional].commit(null)
                      moduleTracer.trace(s"dwr ${h.getClass.getSimpleName} commit done")
                    }
                }
                if (dmOnlineHandlers != null) {
                  dmOnlineHandlers
                    .filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous}
                    .foreach{ h =>
                      moduleTracer.trace(s"dm online ${h.getClass.getSimpleName} commit start")
                      h.asInstanceOf[Transactional].commit(null)
                      moduleTracer.trace(s"dm online ${h.getClass.getSimpleName} commit done")
                    }
                }
                if (dmOfflineHandlers != null) {
                  dmOfflineHandlers
                    .filter{ x => x.isInstanceOf[Transactional] && !x.isAsynchronous}
                    .foreach{ h =>
                      moduleTracer.trace(s"dm offline ${h.getClass.getSimpleName} commit start")
                      h.asInstanceOf[Transactional].commit(null)
                      moduleTracer.trace(s"dm offline ${h.getClass.getSimpleName} commit done")
                    }
                }

                moduleTracer.trace("kafka commit start")
                kafkaClient.commit(kafkaT)
                LOG.warn("kafkaClient offset committed", kafkaT)
                moduleTracer.trace("kafka commit done")

                // Asynchronous handler commit transaction
//                asyncHandlers
//                  .filter{ h => h.isInstanceOf[Transactional] }
//                  .par
//                  .foreach{ h => h.asInstanceOf[Transactional].commit(null) }

                // Wait all asynchronous handler commit transaction
                LOG.warn("wait async handlers handle done")
//                asyncHandlersCountDownLatch.await()
                asyncWorker.await()
                LOG.warn("all async handlers commit done")

                //------------------------------------------------------------------------------------
                // Wait all module transaction commit
                //------------------------------------------------------------------------------------
                LOG.warn(s"wait transaction commit start", "tid", parentTid, "master", isMaster)
                // Key code !!
                moduleTracer.trace("wait master tx commit start", {
                  mixTransactionManager.commitTransaction(isMaster, moduleName, {
                    mixModulesBatchController.completeBatch(isMaster)
                  })
                })
                LOG.warn(s"wait master tx commit done")
                moduleTracer.trace("wait master tx commit done")

                //------------------------------------------------------------------------------------
                // Clean
                //------------------------------------------------------------------------------------
                if (dwi != null) dwi.unpersist()
                if (handledDwi != null) handledDwi.unpersist()
                // Asynchronous and synchronous handler clean
                if(dwiHandlers != null) {
                  dwiHandlers.foreach{ h => h.clean() }
                }
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

                moduleTracer.trace("clean done")

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

//                LOG.warn(moduleName, "clean kafka and set module status idle")
//                GlobalAppRunningStatusV2.setStatus(concurrentGroup, moduleName, GlobalAppRunningStatusV2.STATUS_IDLE)

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
                     |    ${(100.0 * globalConfig.getInt("spark.conf.streaming.batch.buration") / 60).asInstanceOf[Int] / 100.0},
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
//            }
//          }).start()
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

  def startAsyncHandlersCountDownHeartbeats(countDownLatch: CountDownLatch): Unit = {

    ThreadPool.execute {
      var b = true
      while (b) {
        if (countDownLatch.getCount > 0) {
           LOG.warn(s"$moduleName async handler executing count: ${countDownLatch.getCount}")
        } else {
          b = false
        }
        Thread.sleep(1000 * 60)
      }
    }
  }

  def tryKillSelfApp(): Unit = {
    var appId: String = null
    var ms: List[Message] = List()
    if(true/*isMaster*/) {
      MC.pull("kill_self_cer", Array(s"kill_self_$appName"), { x =>
        ms = x
        true
      })
    }

    if (ms.nonEmpty) {
      ms.foreach{y=>
        appId = y.getKeyBody
        LOG.warn(s"Kill self yarn app via user operation !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName)
        YarnAPPManagerUtil.killApps(appName, appId)
      }
    }

  }

  def tryCreateTableForVersionFeatures() = {
    val v = argsConfig.get(ArgsConfig.VERSION, ArgsConfig.Value.VERSION_DEFAULT)
    if(!ArgsConfig.Value.VERSION_DEFAULT.equals(v)) {
      val vSuffix = s"_v${v}"
      LOG.warn("Table major version", v)
      if(StringUtil.notEmpty(dwiTable)) {
        hiveClient.createTableIfNotExists(dwiTable, dwiTable.substring(0, dwiTable.length - vSuffix.length))
//        clickHouseClient.createTableIfNotExists(dwiTable, dwiTable.substring(0, dwiTable.length - vSuffix.length))
//        clickHouseClient.createTableWithEngineIfNotExists(dwiTable + "_all", dwiTable)
//        clickHouseClient.createTableIfNotExists(dwiTable + "_for_select", dwiTable)
//        clickHouseClient.createTableWithEngineIfNotExists(dwiTable + "_for_select_all", dwiTable + "_for_select")
      }
      if(StringUtil.notEmpty(dwrTable) /*&& isMaster*/){
        hiveClient.createTableIfNotExists(dwrTable, dwrTable.substring(0, dwrTable.length - vSuffix.length))
//        clickHouseClient.createTableIfNotExists(dwiTable, dwiTable.substring(0, dwiTable.length - vSuffix.length))
//        clickHouseClient.createTableWithEngineIfNotExists(dwiTable + "_all", dwiTable)
//        clickHouseClient.createTableIfNotExists(dwiTable + "_for_select", dwiTable)
//        clickHouseClient.createTableWithEngineIfNotExists(dwiTable + "_for_select_all", dwiTable + "_for_select")
      }
    }
  }

  def setCommitedOffsetIfSpecified(): Unit ={
    val offset = argsConfig.get(ArgsConfig.OFFSET);
    if(ArgsConfig.Value.OFFSET_EARLIEST.equals(offset)){
//      mySqlJDBCClientV2.execute(s"delete from rollbackable_transaction_cookie where module_name = '${moduleName}'")
//      mySqlJDBCClientV2.execute(s"delete from offset where module_name='${moduleName}'");
      kafkaClient.setOffset(kafkaClient.getEarliestOffset(topics))
    }else if(ArgsConfig.Value.OFFSET_LATEST.equals(offset)) {
      kafkaClient.setOffset(kafkaClient.getLatestOffset(topics))
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
