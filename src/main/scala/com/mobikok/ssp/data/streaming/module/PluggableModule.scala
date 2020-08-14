package com.mobikok.ssp.data.streaming.module

import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.mobikok.message.Message
import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.App
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.OffsetRange
import com.mobikok.ssp.data.streaming.exception.ModuleException
import com.mobikok.ssp.data.streaming.handler.dm
import com.mobikok.ssp.data.streaming.handler.dwi.core.{HBaseDWIPersistHandler, HiveDWIPersistHandler, InitializedDwiHandler}
import com.mobikok.ssp.data.streaming.handler.dwr.core._
import com.mobikok.ssp.data.streaming.module.support.{MixModulesBatchController, TimeGranularity}
import com.mobikok.ssp.data.streaming.module.support.repeats.{BTimeRangeRepeatsFilter, DefaultRepeatsFilter, RepeatsFilter}
import com.mobikok.ssp.data.streaming.transaction.TransactionRoolbackedCleanable
import com.mobikok.ssp.data.streaming.udf.HiveContextCreator
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

  val LOG: Logger = new Logger(moduleName, getClass, new Date().getTime)

  //----------------------  Constants And Fields  -------------------------
  val appName = ssc.sparkContext.getConf.get("spark.app.name")
  val ksc = Class.forName(globalConfig.getString(s"modules.$moduleName.dwi.kafka.schema"))
  val dwiStructType = ksc.getMethod("structType").invoke(ksc.newInstance()).asInstanceOf[StructType]
  var moduleConfig = globalConfig.getConfig(s"modules.$moduleName")
  val dwiUuidFieldsSeparator = "^"

  val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS")

  val shufflePartitions = globalConfig.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  var aggExprs: List[Column] = _
  try {
    aggExprs = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {case _: Exception =>}

  var aggExprsAlias: List[String] = _
  try {
    aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
  } catch {case _: Exception =>}

  var unionAggExprsAndAlias: List[Column] = _
  try {
    unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList
  } catch {case _: Exception =>}

  var businessTimeExtractBy: String = _
  try {
    businessTimeExtractBy = globalConfig.getString(s"modules.$moduleName.b_time.by")
  } catch {case _: Throwable =>}

  var topics = globalConfig.getConfigList(s"modules.$moduleName.kafka.consumer.partitions").map { x => x.getString("topic") }.toSet[String].toArray[String]

  LOG.warn("kafka topics:","topics", topics , "partitions conf path", s"modules.$moduleName.kafka.consumer.partitions")

  var kafkaProtoEnable = false
  var kafkaProtoClass: Class[_] = _
  try {
    kafkaProtoEnable = globalConfig.getBoolean(s"modules.$moduleName.dwi.kafka.proto.enable")
    kafkaProtoClass = Class.forName(globalConfig.getString(s"modules.$moduleName.dwi.kafka.proto.class"))
  } catch {case _: Exception =>}

  val isMaster = mixModulesBatchController.isMaster(moduleName)
  //-------------------------  Constants And Fields End  -------------------------

  //-------------------------  Tools  -------------------------
  val messageClient = new MessageClient(moduleName)
  var moduleTracer: ModuleTracer = new ModuleTracer(moduleName, globalConfig, mixModulesBatchController, messageClient)
  //-------------------------  Tools End  -------------------------

  var stream: InputDStream[ConsumerRecord[String, Object]] = _  //Value Type: String or Array[Byte]
  val transactionManager = mixModulesBatchController.getTransactionManager()

  //-------------------------  Clients  -------------------------
  val hiveContext = HiveContextCreator.create(ssc.sparkContext)

  var mySqlJDBCClient = new MySqlJDBCClient(
    moduleName, globalConfig.getString(s"rdb.url"), globalConfig.getString(s"rdb.user"), globalConfig.getString(s"rdb.password")
  )
  val rDBConfig = new RDBConfig(mySqlJDBCClient)

  var bigQueryClient:BigQueryClient = null
  try {
    bigQueryClient = new BigQueryClient(moduleName, globalConfig, ssc, messageClient, hiveContext, moduleTracer)
  } catch {
    case e: Throwable => LOG.warn("BigQueryClient init fail, Skiped it", s"${e.getClass.getName}: ${e.getMessage}")
  }

  var hbaseClient = new HBaseClient(moduleName, ssc.sparkContext, globalConfig, messageClient, transactionManager, moduleTracer)
  val hiveClient = new HiveClient(moduleName, globalConfig, ssc, messageClient, transactionManager, moduleTracer)
  val kafkaClient = new KafkaClient(moduleName, globalConfig, messageClient, transactionManager, moduleTracer)
  val clickHouseClient = new ClickHouseClient(moduleName, globalConfig, ssc, messageClient, transactionManager, hiveContext, moduleTracer)
  //-------------------------  Clients End  -------------------------

  var dwiHandlers: List[com.mobikok.ssp.data.streaming.handler.dwi.Handler] = _
  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwiBTimeFormat = globalConfig.getString(s"modules.$moduleName.dwi.b_time.format")
  } catch {
    case _: Throwable =>
  }

  //-------------------------  Dwi about  -------------------------
  var isDwiPhoenixStore = false
  try {
    isDwiPhoenixStore = globalConfig.getBoolean(s"modules.$moduleName.dwi.phoenix.store")
  } catch {case _: Exception =>}

  var isDwiStore = false
  var dwiTable: String = _
  try {
    isDwiStore = globalConfig.getBoolean(s"modules.$moduleName.dwi.store")
  } catch {case _: Exception =>}
  try{
    dwiTable = globalConfig.getString(s"modules.$moduleName.dwi.table")
  }catch {case _:Exception =>}

  if(isDwiStore && StringUtil.isEmpty(dwiTable)){
    throw new ModuleException(s"Module ${moduleName} must setting dwi.table value if dwi.store=true")
  }

  // dwi明细去重功能
  var isEnableDwiUuid = false
  var dwiUuidFieldsAlias: String = "rowkey" // 声明dwi表 中用于记录要去重的值 的列名
  var dwiUuidFields: Array[String] = _
  try {
    isEnableDwiUuid = globalConfig.getBoolean(s"modules.$moduleName.dwi.uuid.enable")
  } catch {case _: Exception =>}
  if (isEnableDwiUuid) {
    if(StringUtil.isEmpty(dwiTable)) {
      throw new ModuleException(s"Module ${moduleName} must setting dwi.table value if dwi.uuid.enable=true")
    }
    dwiUuidFields = globalConfig.getStringList(s"modules.$moduleName.dwi.uuid.fields").toArray[String](new Array[String](0))
    try {
      dwiUuidFieldsAlias = globalConfig.getString(s"modules.$moduleName.dwi.uuid.alias")
    } catch {case _: Exception =>}
  }

  var repeatsFilter: RepeatsFilter = _
  if (globalConfig.hasPath(s"modules.$moduleName.uuid.filter.class")) {
    val f = globalConfig.getString(s"modules.$moduleName.uuid.filter.class")
    repeatsFilter = Class.forName(f).newInstance().asInstanceOf[RepeatsFilter]
  } else if(globalConfig.hasPath(s"dwi.uuid.b_time.range")){
    // b_time小时级别
    repeatsFilter = new BTimeRangeRepeatsFilter(dwiBTimeFormat, globalConfig.getStringList("dwi.uuid.b_time.range").map(_.toInt).toList)
  }else {
    repeatsFilter = new DefaultRepeatsFilter(dwiBTimeFormat)
  }
  repeatsFilter.init(moduleName, globalConfig, hiveContext, moduleTracer, dwiUuidFieldsAlias, businessTimeExtractBy, dwiTable)
  //-------------------------  Dwi End  -------------------------

  //-------------------------  Dwr about  -------------------------

  var isDwrStore = false
  var dwrTable: String = _
  try {
    isDwrStore = globalConfig.getBoolean(s"modules.$moduleName.dwr.store")
  } catch {case _: Exception =>}
  if (isDwrStore) {
    dwrTable = globalConfig.getString(s"modules.$moduleName.dwr.table")
  }

  var dwrGroupByExprs: List[Column] = _
  try {
    dwrGroupByExprs = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList
  } catch {case _: Exception =>}

  var dwrGroupByExprsAlias: Array[String] = _
  try {
    dwrGroupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray[String]
  } catch {case _: Exception =>}

  var dwrGroupByExprsAliasCol: List[Column] = _
  try {
    dwrGroupByExprsAliasCol = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => col(x.getString("as"))
    }.toList
  } catch {case _: Exception =>}

  var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
  try {
    dwrBTimeFormat = globalConfig.getString(s"modules.$moduleName.dwr.b_time.format")
  } catch {case _: Throwable =>}

  var dwrIncludeRepeated = true
  if (globalConfig.hasPath(s"modules.$moduleName.dwr.include.repeated")) {
    dwrIncludeRepeated = globalConfig.getBoolean(s"modules.$moduleName.dwr.include.repeated")
  } else if (isEnableDwiUuid) {
    dwrIncludeRepeated = false
  }

  var dwrHandlers: util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler] = _

  //-------------------------  Dwr End  -------------------------

  //-------------------------  Dm about  -------------------------
  var dmLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
  try {
    dmLoadTimeFormat = CSTTime.formatter(globalConfig.getString(s"modules.$moduleName.dm.l_time.format"))
  } catch {
    case _: Exception =>
  }

//  var isEnableDmHandler = false
  var dmHandlers: List[dm.Handler] = _

  // 封装上一次“启动流统计”产生的事务数据的清理操作
  @volatile var hiveRollbackedCleanable, hbaseRollbackedCleanable, kafkaRollbackedCleanable: TransactionRoolbackedCleanable = _

  // 标记是否正在读取kafka数据
  @volatile var isModuleReadingKafka = mutable.Map[String, Boolean]()

  //-----------------------------------------------------------------------------------------------------------------
  // Module Init
  //-----------------------------------------------------------------------------------------------------------------
  override def init(): Unit = {
    try {
      LOG.warn(s"${getClass.getSimpleName} init started", moduleName)

      ddl()

      hiveClient.init()
      hbaseClient.init()
      kafkaClient.init()

      hiveRollbackedCleanable = hiveClient.rollback()
      hbaseRollbackedCleanable = hbaseClient.rollback()
      kafkaRollbackedCleanable = kafkaClient.rollback()
      // 清理上次遗留的事务状态标记，
      // 避免在多次失败的重启中(即this.init()方法成功，但是this.handler()方法有异常导致失败)每次都会先回滚，没必要
      transactionManager.deleteTransactionCookie(moduleName)

      initDwiHandlers()
      initDwrHandlers()
      initDmHandlers()

      tryCreateTableForVersionFeatures()
      resetKafkaCommittedOffsetByArgsConfig()

      if(mixModulesBatchController.isRunnable(moduleName)){
        mySqlJDBCClient.execute(
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
             |    "NA",
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
      }

      initHeartbeat()

      LOG.warn(s"${getClass.getSimpleName} init completed", moduleName)
    } catch {
      case e: Exception =>
        throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' init fail, Transactionals rollback exception", e)
    }
  }

  //-----------------------------------------------------------------------------------------------------------------
  // Module handler
  //-----------------------------------------------------------------------------------------------------------------
  def handler(): Unit = {

    stream = kafkaClient.createDirectStream(globalConfig, ssc, kafkaClient.getCommitedOffset(topics: _*), moduleName)

    stream.foreachRDD { source =>
      try {

        val order = transactionManager.generateTransactionOrder(moduleName)
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

        val dwiCount = offsetDetail.map(_._2).sum
        moduleTracer.trace(s"offset detail cnt: ${dwiCount} lag: ${offsetDetail.map(_._3).sum}\n" + offsetDetail.map { x => s"        ${x._1.topic}, ${x._1.partition} -> cnt: ${x._2} lag: ${x._3}" }.mkString("\n"))

        // 判断是否需要空跑当前批次，需满足下面2个条件:
        // 1) 如果上一个批次还未结束(如果结束了isBeginedTransaction()不会返回true了);
        // 2) 并且当前批次内容为空
        if (transactionManager.isUncompletedTransaction(moduleName) && dwiCount == 0) {

          LOG.warn("Fast polling", "concurrentGroup", concurrentGroup, "moduleName", moduleName)
          moduleTracer.trace("fast polling")

        } else {

          moduleTracer.trace("wait last batch", {
            while (isModuleReadingKafka.getOrElse(moduleName, false)) {
              Thread.sleep(1000)
            }
            isModuleReadingKafka.put(moduleName, true)
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

          var dwi = hiveContext
            .read
            .schema(dwiStructType)
            .json(filtered)

          // 是否开启了去重功能
          if (isEnableDwiUuid) {
            dwi = dwi.select(concat_ws(dwiUuidFieldsSeparator, dwiUuidFields.map { x => expr(x) }: _*).as(dwiUuidFieldsAlias), col("*"))
          } else {
            dwi = dwi.select(expr(s"null as $dwiUuidFieldsAlias"), col("*"))
          }

          dwi = dwi.persist(StorageLevel.MEMORY_ONLY_SER).alias("dwi")
          moduleTracer.trace("read kafka done")
          isModuleReadingKafka.put(moduleName, false)

          val groupName = mixModulesBatchController.getDwrShareTable()

          //-----------------------------------------------------------------------------------------------------------------
          //  Begin Transaction !!
          //-----------------------------------------------------------------------------------------------------------------
          val parentTransactionId = transactionManager.beginTransaction(moduleName, groupName, order)
          val parentThreadId= Thread.currentThread().getId

          val dwrLTimeExpr = s"'${transactionManager.dwrLTime(moduleConfig)}'"
          val asyncWorker = new AsyncHandlerWorker(moduleName, asyncHandlersCount, moduleTracer, order, parentTransactionId,  parentThreadId)

          //-----------------------------------------------------------------------------------------------------------------
          //  DWI Handler
          //-----------------------------------------------------------------------------------------------------------------
          var handledDwi: DataFrame = dwi
          dwiHandlers.foreach{ h =>
            if(h.isAsynchronous){
              asyncWorker.run{
                h.handle(handledDwi)
                h.commit()
              }
            }else{
              handledDwi = h.handle(handledDwi)
              h.commit()
            }
          }
          if (handledDwi != dwi) {
            handledDwi.persist(StorageLevel.MEMORY_ONLY_SER)
          }

          //-----------------------------------------------------------------------------------------------------------------
          //  DWR prepare handle, Each module needs to be executed(Not only 'master=true's module) !
          //-----------------------------------------------------------------------------------------------------------------
          var dwrDwi = handledDwi
          dwrHandlers.foreach { h =>
            dwrDwi = h.prepare(dwrDwi) // 同步和异步的handler都要执行预处理
          }

          var preparedDwr = dwrDwi
          if (dwrHandlers.nonEmpty) {
            preparedDwr = dwrDwi
              .withColumn("l_time", expr(dwrLTimeExpr))
              .withColumn("b_date", to_date(expr(businessTimeExtractBy)).cast("string"))
              .withColumn("b_time", expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwrBTimeFormat')").as("b_time"))
              /*.withColumn("b_version", expr("'0'")) // 默认0*/
              .groupBy(col("l_time") :: col("b_date") :: col("b_time") /*:: col("b_version")*/ :: dwrGroupByExprs: _*)
              .agg(aggExprs.head, aggExprs.tail: _*)

            //-----------------------------------------------------------------------------------------------------------------
            // Wait mix module union dwr data. Key code !!
            //-----------------------------------------------------------------------------------------------------------------
            moduleTracer.trace("wait dwr union all", {
              mixModulesBatchController.waitUnionAll(preparedDwr, isMaster, moduleName)
            })
          }

          //-----------------------------------------------------------------------------------------------------------------
          //  DWR handle
          //-----------------------------------------------------------------------------------------------------------------
          if(isMaster) {
            dwrHandlers.foreach{ h =>
              if(h.isAsynchronous){
                asyncWorker.run{
                  h.handle(mixModulesBatchController.get())
                  h.commit()
                }
              }else {
                mixModulesBatchController.set({
                  h.handle(mixModulesBatchController.get())
                })
                h.commit()
              }

            }
          }

          //-----------------------------------------------------------------------------------------------------------------
          //  DM handle
          //-----------------------------------------------------------------------------------------------------------------
          dmHandlers.foreach{ h =>
            if(h.isAsynchronous){
              asyncWorker.run{
                h.handle()
              }
            }else {
              h.handle()
            }
          }

          //-----------------------------------------------------------------------------------------------------------------
          // Set kafka new offset
          //-----------------------------------------------------------------------------------------------------------------
          val kafkaCookie = kafkaClient.setOffset(parentTransactionId, offsetRanges)
          kafkaClient.commit(kafkaCookie)
          transactionManager.collectTransactionCookie(kafkaClient, kafkaCookie)

          //------------------------------------------------------------------------------------
          // Wait all asynchronous handler done
          //------------------------------------------------------------------------------------
          asyncWorker.await()

          //------------------------------------------------------------------------------------
          // Wait mix module transaction commit, Key code !!
          //------------------------------------------------------------------------------------
          transactionManager.commitTransaction(isMaster, moduleName, moduleTracer, {
            mixModulesBatchController.completeBatch(isMaster)
          })

          //------------------------------------------------------------------------------------
          // Clean transaction tmp data
          //------------------------------------------------------------------------------------
          // 清理上一次用于事务的临时数据
          cleanLastTransactionCookies(dwi, handledDwi, parentTransactionId)
          // 清理上一次“启动流统计”遗留的用于事务的临时数据
          cleanLastTransactionRollbackedCookies()

          moduleTracer.trace("batch done")
          moduleTracer.endBatch()

          mySqlJDBCClient.execute(
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
               |    "NA",
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

          killSelfAppIfNotice()

        }
      } catch {
        case e: Exception => {
          LOG.warn(s"Kill self yarn app, because module '$moduleName' execution failed !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName, "error", e)
          YarnAppManagerUtil.killApps(appName, messageClient)
          throw new ModuleException(s"${getClass.getSimpleName} '$moduleName' execution failed !! ", e)
        }
      }
    }
  }

  def initHeartbeat(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          LOG.warn("Module heartbeat", moduleTracer.getHistoryBatchesTraceResult())
          Thread.sleep(1000 * 60 * 10L) //10分钟打印一次心跳日志
        }
      }
    }).start()
  }

  // 清理上一次用于事务的临时数据
  def cleanLastTransactionCookies(dwi: DataFrame, handledDwi: DataFrame, parentTid: String): Unit ={
    if (dwi != null) dwi.unpersist()
    if (handledDwi != null) handledDwi.unpersist()

    // Handler clean
    if(dwiHandlers != null) {
      dwiHandlers.foreach{ h => h.clean() }
    }
    if (isMaster) {
      dwrHandlers.foreach{h => h.clean()}
    }

    transactionManager.cleanLastTransactionCookie(kafkaClient)

    moduleTracer.trace("clean done")
  }
  // 清理上一次“启动流统计”遗留的事务临时表数据
  def cleanLastTransactionRollbackedCookies(): Unit ={
    if (!transactionManager.needRealTransactionalAction()) {
      if (hiveRollbackedCleanable != null) {
        hiveRollbackedCleanable.doActions()
        hiveRollbackedCleanable = null
      }
      if (hbaseRollbackedCleanable != null) {
        hbaseRollbackedCleanable.doActions()
        hbaseRollbackedCleanable = null
      }
      if (kafkaRollbackedCleanable != null) {
        kafkaRollbackedCleanable.doActions()
        kafkaRollbackedCleanable = null
      }
    }
  }

//  //<cookieKind, <transactionParentId, transactionCookies>>
//  private def cacheTransactionCookies(cookieKind: String, transactionCookie: TransactionCookie): Unit = {
//
//    if (transactionCookie.isInstanceOf[HiveRollbackableTransactionCookie]
//      || transactionCookie.isInstanceOf[KafkaRollbackableTransactionCookie]
//      || transactionCookie.isInstanceOf[HBaseTransactionCookie] // HBaseTransactionCookie待删
//    ) {
//
//      var pr = batchsTransactionCookiesCache.get(cookieKind)
//      if (pr == null) {
//        pr = new util.ArrayList[TransactionCookie]()
//        batchsTransactionCookiesCache.put(cookieKind, pr)
//      }
//      pr.add(transactionCookie)
//    }
//  }

//  private val EMPTY_TRANSACTION_COOKIES = Array[TransactionCookie]()
//
//  //找到上一次事务的临时数据并移除
//  private def popNeedCleanTransactions(cookieKind: String, excludeCurrTransactionParentId: String): Array[TransactionCookie] = {
//    var result = EMPTY_TRANSACTION_COOKIES
//    val pr = batchsTransactionCookiesCache.get(cookieKind)
//    val isTran = transactionManager.needRealTransactionalAction()
//    if (pr != null && isTran) {
//      val needCleans = pr.filter(!_.parentId.equals(excludeCurrTransactionParentId))
//      pr.removeAll(needCleans)
//      result = needCleans.toArray
//    }
//    result
//  }


  var asyncHandlersCount = 0
  def initDwiHandlers(): Unit = {
    dwiHandlers = List()

    val initializedHandler = new InitializedDwiHandler(repeatsFilter, businessTimeExtractBy, isEnableDwiUuid, dwiBTimeFormat, argsConfig)
    initializedHandler.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, messageClient, moduleTracer)

    dwiHandlers = dwiHandlers :+ initializedHandler

    // Config eg: dwi.handlers = [{class=""}]
    if (globalConfig.hasPath(s"modules.$moduleName.dwi.handlers")) {
      dwiHandlers = dwiHandlers ++ globalConfig.getConfigList(s"modules.$moduleName.dwi.handlers").map { x =>
        val hc = x
        val className = hc.getString("class")
        val h = Class.forName(className).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwi.Handler]
        h.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, hc, globalConfig, messageClient, moduleTracer)
        LOG.warn("Init dwi handler", h.getClass.getName)
        h
      }.toList
    }

    // 核心常用handler的快捷配置，无需在配置文件里指定handler类名
    if (isDwiStore) {
      val hiveDWIPersistHandler = new HiveDWIPersistHandler()
      hiveDWIPersistHandler.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, messageClient, moduleTracer)
      dwiHandlers = dwiHandlers :+ hiveDWIPersistHandler
    }
    // 核心常用handler的快捷配置，无需在配置文件里指定handler类名
    if (isDwiPhoenixStore) {
      val hbaseDWIPersistHandler = new HBaseDWIPersistHandler()
      hbaseDWIPersistHandler.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, null, globalConfig, messageClient, moduleTracer)
      dwiHandlers = dwiHandlers :+ hbaseDWIPersistHandler
    }

    asyncHandlersCount += dwiHandlers.map{ x=>if(x.isAsynchronous) 1 else 0}.sum
  }

  def initDwrHandlers(): Unit ={
    dwrHandlers = new util.ArrayList[com.mobikok.ssp.data.streaming.handler.dwr.Handler]()
    if (!dwrIncludeRepeated) {
      val h = new NonRepeatedPrepareDwrHandler(repeatsFilter)
      h.init(moduleName, "", transactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, messageClient, moduleTracer)
      dwrHandlers.add(h)
    }

    // Config eg: dwr.handlers = [{class=""}]
    if(globalConfig.hasPath(s"modules.$moduleName.dwr.handlers")){
      globalConfig.getConfigList(s"modules.$moduleName.dwr.handlers").foreach { x =>
        val hc = x
        val h = Class.forName(hc.getString("class")).newInstance().asInstanceOf[com.mobikok.ssp.data.streaming.handler.dwr.Handler]
        h.init(moduleName, null, transactionManager, hbaseClient, hiveClient, clickHouseClient, hc, globalConfig, messageClient, moduleTracer)
        dwrHandlers.add(h)
      }
    }

    // 核心常用handler的快捷配置，无需在配置文件里指定handler类名
    if (isDwrStore) {
      if(globalConfig.hasPath(s"modules.$moduleName.dwr.subtables")){
        globalConfig.getConfig(s"modules.$moduleName.dwr.subtables").root().foreach{case(subTableSuffix, _)=>

          val (subDwrTable, groupByFields, where, timeGranularity) = parseSubDwrTablesConfig(moduleName, subTableSuffix)

          val dwrPersistHandler = new HiveDWRPersistHandler(dwrTable, subDwrTable, groupByFields, where, timeGranularity)
          dwrPersistHandler.init(moduleName, subTableSuffix , transactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, messageClient, moduleTracer)
          dwrHandlers.add(dwrPersistHandler)
        }
      }else {
        val dwrPersistHandler = new HiveDWRPersistHandler(dwrTable, Array("*"), null, TimeGranularity.Default)
        dwrPersistHandler.init(moduleName, null, transactionManager, hbaseClient, hiveClient, clickHouseClient, null, globalConfig, messageClient, moduleTracer)
        dwrHandlers.add(dwrPersistHandler)
      }
    }

    if(isMaster){
      asyncHandlersCount += dwrHandlers.map{ x=>if(x.isAsynchronous) 1 else 0}.sum
    }
  }

  private def parseSubDwrTablesConfig(moduleName: String, subTableSuffix: String): (String, Array[String], String, TimeGranularity) ={
    val ts = s"modules.$moduleName.dwr.subtables"
    val SUB_TABLE_DEFAULT = "this" // this表示配置的dwr.table就是子表自身
    (
      // subTable
      if (SUB_TABLE_DEFAULT.equals(subTableSuffix)) dwrTable else s"${dwrTable}_${subTableSuffix}",

      // groupByFields
      if(globalConfig.hasPath(s"$ts.${subTableSuffix}.groupby"))
        globalConfig.getString(s"$ts.${subTableSuffix}.groupby").split(",").map(_.trim).filter(StringUtil.notEmpty(_))
      else Array("*"),

      // where
      if(globalConfig.hasPath(s"$ts.${subTableSuffix}.where")) globalConfig.getString(s"$ts.${subTableSuffix}.where") else null,

      // timeGranularity
      if(subTableSuffix.startsWith(TimeGranularity.Day.name))
        TimeGranularity.Day
      else if(subTableSuffix.startsWith(TimeGranularity.Month.name))
        TimeGranularity.Month
      else TimeGranularity.Default
    )
  }

  def initDmHandlers(): Unit ={
    dmHandlers = List()

    // Config eg: dm.handlers = [{class=""}]
    if(globalConfig.hasPath(s"modules.$moduleName.dm.handlers")){
      if (!isMaster) {
        LOG.warn(s"Error handler '${dmHandlers.head.getClass.getName}': Module must setting the module 'master=true' if include 'dm.handler'")
        throw new ModuleException("Module must setting the module master=true if include 'dm.handlers'")
      }

      dmHandlers = globalConfig.getConfigList(s"modules.$moduleName.dm.handlers").map { handlerConfig =>
        val h = Class.forName(handlerConfig.getString("class")).newInstance().asInstanceOf[dm.Handler]
        h.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)
        h
      }.toList
    }

    asyncHandlersCount += dmHandlers.map{ x=>if(x.isAsynchronous) 1 else 0}.sum
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

  def killSelfAppIfNotice(): Unit = {
    var appId: String = null
    var ms: List[Message] = List()
    if(true/*isMaster*/) {
      messageClient.pull("kill_self_cer", Array(s"kill_self_$appName"), { x =>
        ms = x
        true
      })
    }

    if (ms.nonEmpty) {
      ms.foreach{y=>
        appId = y.getKeyBody
        LOG.warn(s"Kill self yarn app via user operation !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", appName)
        YarnAppManagerUtil.killApps(appName, appId, messageClient)
      }
    }

  }

  def tryCreateTableForVersionFeatures() = {
    val v = argsConfig.get(ArgsConfig.VERSION, ArgsConfig.Value.VERSION_DEFAULT)
    if(!ArgsConfig.Value.VERSION_DEFAULT.equals(v)) {
      val vSuffix = s"_v${v}"
      LOG.warn("Table version", v)
      if(StringUtil.notEmpty(dwiTable)) {
        hiveClient.createTableIfNotExists(dwiTable, dwiTable.substring(0, dwiTable.length - vSuffix.length))
      }
      if(StringUtil.notEmpty(dwrTable) /*&& isMaster*/){
        hiveClient.createTableIfNotExists(dwrTable, dwrTable.substring(0, dwrTable.length - vSuffix.length))
      }
    }
  }

  def resetKafkaCommittedOffsetByArgsConfig(): Unit ={
    val offset = argsConfig.get(ArgsConfig.OFFSET);
    if(ArgsConfig.Value.OFFSET_EARLIEST.equals(offset)){
      kafkaClient.setOffset(kafkaClient.getEarliestOffset(topics))
    }else if(ArgsConfig.Value.OFFSET_LATEST.equals(offset)) {
      kafkaClient.setOffset(kafkaClient.getLatestOffset(topics))
    }
  }

  private def ddl(): Unit ={
    mySqlJDBCClient.execute(
      """
        |CREATE TABLE IF NOT EXISTS `module_running_status`  (
        |  `module_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
        |  `batch_using_time` double NULL DEFAULT NULL,
        |  `batch_actual_time` double NULL DEFAULT NULL,
        |  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        |  `app_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
        |  `batch_using_time_trace` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
        |  `rebrush` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'true/false',
        |  `batch_buration` double NULL DEFAULT NULL,
        |  `lag` bigint(20) NULL DEFAULT NULL,
        |  PRIMARY KEY (`module_name`) USING BTREE
        |) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
      """.stripMargin)
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
