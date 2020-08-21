package com.mobikok.ssp.data.streaming

import java.io.File
import java.text.SimpleDateFormat
import java.util.Collections

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.App.{appName, argsConfig}
import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, LatestOffsetRecord, UuidStat}
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.module.Module
import com.mobikok.ssp.data.streaming.module.support.MixModulesBatchController
import com.mobikok.ssp.data.streaming.schema.dwi.kafka.EmptyDWISchema
import com.mobikok.ssp.data.streaming.transaction.{OptimizedTransactionalStrategy, TransactionManager}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.immutable.Nil

/**
  * Created by Administrator  on 2017/6/8.
  */
class App {
}

object App {

  private[this] val LOG = Logger.getLogger(getClass().getName())
  var ssc: StreamingContext = null
  var mySqlJDBCClient: MySqlJDBCClient = null
  var allModulesConfig: Config = null
  var runnableModulesConfig: Config = null
  var argsConfig: ArgsConfig = new ArgsConfig()
  val dataFormat: SimpleDateFormat = CSTTime.formatter("yyyyMMdd-HHmmss") //new SimpleDateFormat("yyyyMMdd-HHmmss")
  //加载获取spark-submit启动命令中参数配置
  var sparkConf: SparkConf = null
  var appName: String = null
  var appId: String = null
  var version: String = null
  var kafkaTopicVersion: String = null
  val kafkaOffsetTool = new KafkaOffsetTool(App.getClass.getSimpleName)

  var dwiLTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:00:00")
  var dwrLTimeFormat = CSTTime.formatter("yyyy-MM-dd 00:00:00")
  //table, list<module>
  private var shareTableModulesControllerMap: java.util.Map[String, MixModulesBatchController] = null
  var messageClient: MessageClient = null

  def main (args: Array[String]): Unit = {
    try {

      loadConfigFile(args)
      // 初始化流统计配置与实例化相关Module
      initSparkStreaming()
      startSparkStreaming()

    }catch {case e:Throwable=>
      throw new AppException(s"${this.getClass.getSimpleName}(${this.appName}) run fail !!", e)
    }
  }

  def startSparkStreaming(){
    ssc.start
    ssc.awaitTermination
  }

  def loadConfigFile(args: Array[String]){
    sparkConf = new SparkConf(true)
    //      获取spark-submit启动命令中的--name参数指定的
    appName = sparkConf.get("spark.app.name").trim
    appId = YarnAppManagerUtil.getLatestRunningApp(appName).getApplicationId.toString

    LOG.warn("Starting App", "name", appName, "id", appId)

    if(args.length > 0) {
      var f = new File(args(0))
      if(!f.exists()) {
        throw new AppException(s"The modules config file '${args(0)}' does not exist")
      }

      allModulesConfig = ConfigFactory.parseFile(f)//load(args(0))

      LOG.warn(s"Load Config File (exists: ${f.exists()}): " + args(0) + "\nsetting plain:" +allModulesConfig.root().unwrapped().toString + s"\nargs config plain: ${java.util.Arrays.deepToString(args.tail.asInstanceOf[Array[Object]])}")

      argsConfig = new ArgsConfig().init(args.tail)
      version = argsConfig.get(ArgsConfig.VERSION, ArgsConfig.Value.VERSION_DEFAULT)
      kafkaTopicVersion = argsConfig.get(ArgsConfig.KAFKA_TOPIC_VERSION, ArgsConfig.Value.KAFKA_TOPIC_VERSION_DEFAULT)

      if(!ArgsConfig.Value.VERSION_DEFAULT.equals(version) && !appName.endsWith(s"v$version")) throw new IllegalArgumentException(s"App name suffix must be: v$version, Suggests app name: ${appName}_v${version}")

      LOG.warn("\nParsed ArgsConfig: \n" + argsConfig.toString)

    }else {
      LOG.warn("Load Config File In Classpath")
      allModulesConfig = ConfigFactory.load
    }
  }

  def initSparkStreaming() = {
    initLoggerLevel()
    initModulesConfig()
    initMessageClient()
    initRDBConfig()
    initStreamingContext()
    initAllModulesInstances()
  }

  def generateMixMoudlesBatchController(hiveContext: HiveContext, shufflePartitions:Int, moduleName: String, runnableModuleNames: Array[String]): MixModulesBatchController ={

    if(shareTableModulesControllerMap == null) {
      shareTableModulesControllerMap = new java.util.HashMap[String, MixModulesBatchController]()
      var ms: Config = null
      try {
        ms = allModulesConfig.getConfig("modules")
      }catch {case e:Exception=>}

      if(ms != null) {
        ms.root().foreach{x=>
          var t:String = null
          val m = x._1
          var isM = false
          try {
            if(allModulesConfig.getBoolean(s"modules.${m}.dwr.store")) {
              t = allModulesConfig.getString(s"modules.${m}.dwr.table")
            }
          }catch {case e:Exception=>
            LOG.warn("Get the dwr table name failed, Caused by " + e.getMessage)
          }
          if(t == null) {
            t = s"non_sharing_dwr_table_module_$m"
          }
          try{
            isM = allModulesConfig.getBoolean(s"modules.$m.master")
          }catch {case e:Exception=>}

          //---------------------------------- Generate l_time DateFormat START ---------------------------------
          try {
            dwiLTimeFormat =  CSTTime.formatter(allModulesConfig.getString(s"modules.$m.dwi.l_time.format")) //DateFormatUtil.CST(allModulesConfig.getString(s"modules.$m.dwi.l_time.format"))
          }catch { case _: Exception =>}

          try {
            dwrLTimeFormat = CSTTime.formatter(allModulesConfig.getString(s"modules.$m.dwr.l_time.format"))//DateFormatUtil.CST(allModulesConfig.getString(s"modules.$m.dwr.l_time.format"))
          }catch {case _:Exception => }

          //---------------------------------- Generate l_time DateFormat END ---------------------------------

          var cer = shareTableModulesControllerMap.get(t)
          if(cer == null) {
            cer = new MixModulesBatchController(allModulesConfig, runnableModuleNames, t, new TransactionManager(allModulesConfig, new OptimizedTransactionalStrategy(dwiLTimeFormat, dwrLTimeFormat, t), t), hiveContext, shufflePartitions)
            shareTableModulesControllerMap.put(t, cer)
          }else {
            if(!dwrLTimeFormat.toPattern.equals(cer.getTransactionManager().dwrLTimeDateFormat().toPattern) || !dwiLTimeFormat.toPattern.equals(cer.getTransactionManager().dwiLTimeDateFormat().toPattern)) {
              throw new AppException(
                s"""Moudle '$m' config is wrong, Share same dwr table moudles must be the same 'dwi.l_time.format' and same 'dwr.l_time.format', Detail:
                   |dwrLoadTimeFormat: ${dwrLTimeFormat.toPattern}
                   |cer.dwrLoadTimeDateFormat: ${cer.getTransactionManager().dwrLTimeDateFormat().toPattern}
                   |dwiLoadTimeFormat: ${dwiLTimeFormat.toPattern}
                   |cer.dwiLoadTimeDateFormat: ${cer.getTransactionManager().dwiLTimeDateFormat().toPattern}
                 """.stripMargin
              )
            }
          }
          cer.addModule(m, isM)
        }

        //效验
        shareTableModulesControllerMap.entrySet().foreach{x=>
          x.getValue.assertJustOnlyOneMasterModule()
        }

      }

    }

    for(e <- shareTableModulesControllerMap.entrySet()){
      if(e.getValue.isContainsModule(moduleName)) {
        return e.getValue
      }
    }

    throw new AppException(s"Cannot generate MoudlesShareTableBatchController instance for module: $moduleName, shareTableModulesControllerMap: ${shareTableModulesControllerMap.toList.map{x=>(x._1, x._2.modules().toList)}}")
  }

  def initLoggerLevel(): Unit ={
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("com.mobikok.ssp.data.streaming").setLevel(Level.DEBUG)
    //    Logger.getLogger(classOf[Hive]).setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.streaming.kafka010").setLevel(Level.INFO)

    Logger.getLogger("org.spark-project.jetty").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project.jetty.util.component.AbstractLifeCycle").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.repl.SparkIMain$exprTyper").setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.repl.SparkILoop$SparkILoopInterpreter").setLevel(Level.INFO)

    Logger.getLogger("org.apache.spark.sql.hive.HiveExternalCatalog").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.hadoop.hbase.MetaTableAccessor").setLevel(Level.ERROR)
  }

  def initRDBConfig(): Unit ={
    mySqlJDBCClient = new MySqlJDBCClient(getClass.getSimpleName, allModulesConfig.getString(s"rdb.url"), allModulesConfig.getString(s"rdb.user"), allModulesConfig.getString(s"rdb.password"))
    RDBConfig.init(mySqlJDBCClient)
  }

  def initStreamingContext(): Unit ={
    val conf = initSparkConf()

    conf.registerKryoClasses(Array(
      classOf[UuidStat],
      classOf[com.mobikok.ssp.data.streaming.util.Logger],
      classOf[org.apache.log4j.Logger],
      classOf[org.apache.spark.sql.Column]
    ))

    ssc = new StreamingContext(conf, Seconds(allModulesConfig.getInt("spark.conf.streaming.batch.buration")))

    //check task Whether has been launched.
    if(YarnAppManagerUtil.isAppRunning(appName)){
      if("true".equals(argsConfig.get(ArgsConfig.FORCE_KILL_PREV_REPEATED_APP))) {
        YarnAppManagerUtil.killApps(appName, false, appId, messageClient)
      }else {
        throw new RuntimeException(s"This app '$appName' has already submit,forbid to re-submit!")
      }
    }
  }

  def initAllModulesInstances(): Unit ={
    initAllModulesInstances0() { case (/*concurrentGroup, */moduleName, moduleClass/*, structType*/) =>

      val shufflePartitions = allModulesConfig.getInt("spark.conf.set.spark.sql.shuffle.partitions")
      val runnableModuleNames = runnableModulesConfig.getConfig("modules").root.map(_._1).toArray
      var cer = generateMixMoudlesBatchController(new HiveContext(ssc.sparkContext), shufflePartitions, moduleName, runnableModuleNames)


      val m: Module = Class
        .forName(moduleClass)
        .getConstructor(classOf[Config], argsConfig.getClass, /*concurrentGroup.getClass,*/ cer.getClass, moduleName.getClass, runnableModuleNames.getClass, ssc.getClass)
        .newInstance(allModulesConfig, argsConfig, /*concurrentGroup,*/ cer, moduleName, runnableModuleNames, ssc).asInstanceOf[Module]

      val isInitable = cer.isInitable(moduleName)
      val isRunnable = cer.isRunnable(moduleName)
      LOG.warn(s"Module is instanced !! \nmoduleName: $moduleName, \ninitable: $isInitable, \nrunnable: $isRunnable\n")

      if(isInitable){
        m.init
      }

      if(isRunnable) {
        m.handler
      }
    }

  }

  def initSparkConf (): SparkConf ={

//    //加载获取spark-submit启动命令中参数配置
//    val conf = new SparkConf(true)

    // 获取spark-submit启动命令中的--name参数指定的
//    val cmdAppName = sparkConf.get("spark.app.name").trim
    // 一律用spark-submit启动命令中的--name参数值作为app name
    allModulesConfig = allModulesConfig.withValue("spark.conf.set.spark.app.name", ConfigValueFactory.fromAnyRef(appName))

    LOG.info("Final Spark Conf: \n" + allModulesConfig
      .getConfig("spark.conf.set")
      .entrySet()
      .map { x =>
        x.getKey + "=" + x.getValue.unwrapped.toString
      }.mkString("\n", "\n", "\n")
    )

    sparkConf.setAll(allModulesConfig
      .getConfig("spark.conf.set")
      .entrySet()
      .map { x =>
        (x.getKey, x.getValue.unwrapped.toString)
      })

    if(getClass.getName.equals(appName)) {
      throw new AppException(s"Spark app name cannot be a main class name '$appName' ")
    }
    if(StringUtil.isEmpty(appName)) {
      throw new AppException("Spark app name not specified !!")
    }

//    // 最终还是采用spark-submit启动命令中的--name参数值
//    conf.setAppName(cmdAppName)

    sparkConf
  }

  def initMessageClient(): Unit ={
    MessageClient.init(allModulesConfig.getString("message.client.url"))
    messageClient  = new MessageClient(App.getClass.getSimpleName)
  }

  def initModulesConfig(): Unit ={
    var refs: java.util.List[String]= null
    var ms: Config = null

    try {
      ms = allModulesConfig.getConfig("modules")
    } catch {case e:Exception=>
      throw new AppException("No modules be configured !!!", e)
    }

    ms.root().foreach{x=>
      val name = x._1
      val vName = versionFeaturesModuleName(version, name)

      if(!ArgsConfig.Value.VERSION_DEFAULT.equals(version)) {

        // 给module nmae加上对应version信息
        allModulesConfig = allModulesConfig.withValue(s"modules.${vName}", x._2)

        if(allModulesConfig.hasPath(s"modules.${x._1}.dwi.table"))
          allModulesConfig = allModulesConfig.withValue(
            s"modules.${vName}.dwi.table",
            ConfigValueFactory.fromAnyRef(versionFeaturesTableName(version,  allModulesConfig.getValue(s"modules.${x._1}.dwi.table").unwrapped().toString)))

        if(allModulesConfig.hasPath(s"modules.${x._1}.dwr.table"))
          allModulesConfig = allModulesConfig.withValue(
            s"modules.${vName}.dwr.table",
            ConfigValueFactory.fromAnyRef(versionFeaturesTableName(version,  allModulesConfig.getValue(s"modules.${x._1}.dwr.table").unwrapped().toString)))

        // 清理
        allModulesConfig = allModulesConfig.withoutPath(s"modules.${name}")
      }

      // dm handler层module离线调度相关，套用流统计的配置启动离线调度
      // 免去手动在配置文件里配置kafka.consumer.partitions这些固定的值，自动在这里补全配置
      ms.root().foreach{x=>
        // 自动补全为: kafka.consumer {partitions = [{topic = "topic_empty"}]}
        if(!allModulesConfig.hasPath(s"modules.${vName}.kafka.consumer.partitions")){
          allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.kafka.consumer.partitions", ConfigValueFactory.fromIterable(Collections.singleton(Collections.singletonMap("topic", "topic_empty") )))
          allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.dwi.kafka.schema", ConfigValueFactory.fromAnyRef(classOf[EmptyDWISchema].getName))
          if(!allModulesConfig.hasPath(s"modules.${vName}.b_time.by")){
            allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.b_time.by", ConfigValueFactory.fromAnyRef("empty_time"))
          }
        }
      }

      // 给kafka partitions中的topic加上对应的version信息
      var vTps = allModulesConfig.getConfigList(s"modules.${vName}.kafka.consumer.partitions")
          .map{y=>
            var tp = new java.util.HashMap[String, Any]()
            tp.put("topic", versionFeaturesKafkaTopicName(kafkaTopicVersion, y.getString("topic")))
            if(y.hasPath("partition")) tp.put("partition", y.getInt("partition"))
            tp
        }
      allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.kafka.consumer.partitions", ConfigValueFactory.fromIterable(vTps))

      // 读取kafka服务器，获取tipic对应的partition并补全所有分区配置
      var tps = allModulesConfig
        .getConfigList(s"modules.${vName}.kafka.consumer.partitions")
        .filter{y=> !y.hasPath("partition")}
        .map{y=> y.getString("topic")}
        .map{y=> kafkaOffsetTool.getTopicPartitions(allModulesConfig.getString("kafka.consumer.set.bootstrap.servers"), java.util.Collections.singletonList(y))}
        .flatMap{y=> y}
        .map{y=> y.asTuple}
        .union(allModulesConfig.getConfigList(s"modules.${vName}.kafka.consumer.partitions")
          .filter{y=> y.hasPath("partition")}
          .map{y=> (y.getString("topic"), y.getInt("partition"))})
        .distinct
        .map{y=>
          var tp = new java.util.HashMap[String, Any]()
          tp.put("topic", y._1)
          tp.put("partition", y._2)
          tp
        }
      allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.kafka.consumer.partitions", ConfigValueFactory.fromIterable(tps))

      if(argsConfig.has(ArgsConfig.EX)) {
        //拿到用户配置的ex
        val exDims = argsExDimColmunNames(argsConfig).map(_.trim).toSet
        if(allModulesConfig.hasPath(s"modules.${vName}.dwr.groupby.fields")){
          // 排除dwr.groupby.fields中不需要统计的字段
          var fields = allModulesConfig
            .getConfigList(s"modules.${vName}.dwr.groupby.fields")
            .map{y=>
              var filed = new java.util.HashMap[String, String]()
              // 复制字段
              y.root().foreach{case(key, value)=>
                filed.put(key, value.unwrapped().toString)
              }
              // 重置字段值
              if(exDims.contains(y.getString("as"))){
                filed.put("expr", "null")
              }else{
                filed.put("expr", y.getString("expr"))
              }
//                filed.put("as", y.getString("as"))
              filed
            }
          allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.dwr.groupby.fields", ConfigValueFactory.fromIterable(fields))
        }
      }
    }

    //reset buration by command argsConfig: buration=?
    try{
      if(argsConfig.has(ArgsConfig.STREAMING_BATCH_BURATION)) {
        LOG.warn(s"reset duration by command arg-config buration=${argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION)}")
        allModulesConfig = allModulesConfig.withValue("spark.conf.streaming.batch.buration", ConfigValueFactory.fromAnyRef(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION)) )
      }
    }catch {
      case e:Exception=>
        throw new AppException("buration config error !!!", e)
    }

    //reset maxRatePerPartition by command argsConfig: rate=?
    try{
      if(argsConfig.has(ArgsConfig.RATE)) {
        LOG.warn(s"reset maxRatePerPartition by command arg-config rate=${argsConfig.get(ArgsConfig.RATE)}")

        val partitionNum = needRunModuleNames(argsConfig).map { x =>
          allModulesConfig.getList(s"modules.${x}.kafka.consumer.partitions").size()
        }.sum

        /*val partitionNum = allModulesConfig.getConfig("modules").root().map{x =>
          allModulesConfig.getList(s"modules.${x._1}.kafka.consumer.partitions").size()
        }.max*/

        val maxRatePerPartition = Math.ceil(java.lang.Double.valueOf(argsConfig.get(ArgsConfig.RATE))/Integer.valueOf(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION))/partitionNum).toInt

        LOG.warn("reset maxRatePerPartition ", "partitionNum ",partitionNum, "maxRatePerPartition ", maxRatePerPartition)
        allModulesConfig = allModulesConfig.withValue("spark.conf.set.spark.streaming.kafka.maxRatePerPartition", ConfigValueFactory.fromAnyRef(maxRatePerPartition))
      }
    }catch {
      case e:Exception=>
        throw new AppException("No maxRatePerPartition can be configured !!!", e)
    }

    // 需要定义新的特性，请在上面定义

    runnableModulesConfig = allModulesConfig

    // 去掉不需要运行的模块配置
    if(argsConfig.has(ArgsConfig.MODULES)) {
      val rms = needRunModuleNames(argsConfig)

      allModulesConfig.getConfig("modules").root().foreach{ x=>
        if(!rms.contains(x._1)) {
          runnableModulesConfig = runnableModulesConfig.withoutPath(s"modules.${x._1}")
        }
      }

      rms.foreach {x =>
        try{
          //验证模块是否配置
          runnableModulesConfig.getConfig(s"modules.${x}")
        }catch {case e:Throwable=>
          throw new AppException(s"Get module '${x}' config fail", e)
        }
      }
    }

    var concurrents = ConfigValueFactory.fromAnyRef(runnableModulesConfig.getConfig("modules").root().size())
    allModulesConfig = allModulesConfig.withValue("spark.conf.set.spark.streaming.concurrentJobs", concurrents)
    runnableModulesConfig = runnableModulesConfig.withValue("spark.conf.set.spark.streaming.concurrentJobs", concurrents)

    try {
      ms = runnableModulesConfig.getConfig("modules")
    } catch {case e:Exception=>
      throw new AppException("No runnable modules be configured !!!", e)
    }

    LOG.warn("\nApp runnable modules final config content:\n" + runnableModulesConfig.root().unwrapped().toString +"\n")

  }

  def argsExDimColmunNames(argsConfig: ArgsConfig):Array[String] = {
    argsConfig.get(ArgsConfig.EX).split(",")
  }

  def needRunModuleNames(argsConfig: ArgsConfig): Array[String] ={
    val ns = argsModuleNames(argsConfig).map(_.trim).distinct
    if(ns.isEmpty) allModulesConfig.getConfig("modules").root().map{case(moduleName, _)=> moduleName}.toArray
    else ns

  }
  def argsModuleNames(argsConfig: ArgsConfig):Array[String] = {
    if(argsConfig.has(ArgsConfig.MODULES)) argsConfig.get(ArgsConfig.MODULES).split(",").map(x=>versionFeaturesModuleName(version, x))
    else Array.empty[String]
  }

  def versionFeaturesModuleName(version: String, moduleName: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) moduleName else s"${moduleName}_v${version}".trim
  }

  def versionFeaturesTableName(version: String, table: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) table else s"${table}_v${version}".trim
  }

  def versionFeaturesKafkaTopicName(kafkaTopicVersion: String, kafkaTopic: String): String = {
    return if(ArgsConfig.Value.KAFKA_TOPIC_VERSION_DEFAULT.equals(kafkaTopicVersion)) kafkaTopic else s"${kafkaTopic}_v${kafkaTopicVersion}".trim
  }

  def initAllModulesInstances0()(createModule: (/*String, */String, String/*, StructType*/) => Unit) : Unit = {
    // 初始化与当前runnable模块关联同一个dwr表的模块
    allModulesConfig.getConfig("modules").root.map{ case(moduleName, _)=>
        initModuleInstance(/*moduleName,*/ moduleName, createModule)
    }.toArray

  }

  private def initModuleInstance(/*concurrentGroup: String, */moduleName: String, createModule: (/*String, */String, String/*, StructType*/) => Unit): Unit = {
    val n = moduleName
    val mc = allModulesConfig.getString(s"modules.${n}.class")
    createModule(/*concurrentGroup, */n, mc)
  }


}
//object X{
//  def main (args: Array[String]): Unit = {
//    val refC = ConfigFactory.parseFile(new File("C:\\Users\\Administrator\\IdeaProjects\\datastreaming\\src\\main\\scala\\dw\\fee.conf"))//load(args(0))
//
//   //println(refC.getConfig("modules").withValue("asd", refC.getValue("")))
//  }
//}