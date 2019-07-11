package com.mobikok.ssp.data.streaming

import java.io.File
import java.text.SimpleDateFormat

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.MixApp.{argsConfig, config}
import com.mobikok.ssp.data.streaming.OptimizedMixApp.{appName, argsConfig}
import com.mobikok.ssp.data.streaming.client.{HBaseMultiSubTableClient, MixTransactionManager}
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, LatestOffsetRecord, UuidStat}
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.module.Module
import com.mobikok.ssp.data.streaming.module.support.{MixModulesBatchController, OptimizedTransactionalStrategy}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * Created by Administrator  on 2017/6/8.
  */
class OptimizedMixApp {

}

object OptimizedMixApp {

  private[this] val LOG = Logger.getLogger(getClass().getName())
  var allModulesConfig: Config = null
  var runnableModulesConfig: Config = null
  var argsConfig: ArgsConfig = new ArgsConfig()
  val dataFormat: SimpleDateFormat = CSTTime.formatter("yyyyMMdd-HHmmss") //new SimpleDateFormat("yyyyMMdd-HHmmss")
  //加载获取spark-submit启动命令中参数配置
  var sparkConf: SparkConf = null
  var appName: String = null
  var appId: String = null
  var version: String = null

  var dwiLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:00:00")
  var dwrLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd 00:00:00")

  def main (args: Array[String]): Unit = {
    try {

      sparkConf = new SparkConf(true)
//      获取spark-submit启动命令中的--name参数指定的
      appName = sparkConf.get("spark.app.name").trim
      appId = YarnAPPManagerUtil.getLatestRunningApp(appName).getApplicationId.toString

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

        if(!ArgsConfig.Value.VERSION_DEFAULT.equals(version) && !appName.endsWith(s"v$version")) throw new IllegalArgumentException(s"App name suffix must be: v$version, App name suggests: ${appName}_v${version}")

        LOG.warn("\nParsed ArgsConfig: \n" + argsConfig.toString)

      }else {
        LOG.warn("Load Config File In Classpath")
        allModulesConfig = ConfigFactory.load
      }

      val ssc = init()

      //    ssc.checkpoint(config.getString("spark.conf.streaming.checkpoint"))
      if(hasSparkStreaming){
        ssc.start
        ssc.awaitTermination
      }else{
        while(true) {
          Thread.sleep(1000)
        }
      }

    }catch {case e:Throwable=>

      throw new AppException("AppV2 run fail !!", e)
    }
  }

  //table, list<module>
  private var shareTableModulesControllerMap = null.asInstanceOf[java.util.HashMap[String, MixModulesBatchController]]

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
            if(allModulesConfig.getBoolean(s"modules.${m}.dwr.enable")) {
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
            dwiLoadTimeFormat =  CSTTime.formatter(allModulesConfig.getString(s"modules.$m.dwi.load.time.format.by")) //DateFormatUtil.CST(allModulesConfig.getString(s"modules.$m.dwi.load.time.format.by"))
          }catch { case _: Exception =>}
          if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
            dwiLoadTimeFormat = CSTTime.formatter(dwiLoadTimeFormat.toPattern.substring(0, 17) + "01")//DateFormatUtil.CST(dwiLoadTimeFormat.toPattern.substring(0, 17) + "01")
          }

          try {
            dwrLoadTimeFormat = CSTTime.formatter(allModulesConfig.getString(s"modules.$m.dwr.load.time.format.by"))//DateFormatUtil.CST(allModulesConfig.getString(s"modules.$m.dwr.load.time.format.by"))
          }catch {case _:Exception => }
          if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
            dwrLoadTimeFormat = CSTTime.formatter(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")//new SimpleDateFormat(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")
          }
          //---------------------------------- Generate l_time DateFormat END ---------------------------------

          var cer = shareTableModulesControllerMap.get(t)
          if(cer == null) {
            cer = new MixModulesBatchController(allModulesConfig, runnableModuleNames, t, new MixTransactionManager(allModulesConfig, new OptimizedTransactionalStrategy(dwiLoadTimeFormat, dwrLoadTimeFormat)), hiveContext, shufflePartitions)
            shareTableModulesControllerMap.put(t, cer)
          }else {
            if(!dwrLoadTimeFormat.toPattern.equals(cer.getMixTransactionManager().dwrLoadTimeDateFormat().toPattern) || !dwiLoadTimeFormat.toPattern.equals(cer.getMixTransactionManager().dwiLoadTimeDateFormat().toPattern)) {
              throw new AppException(
                s"""Moudle '$m' config is wrong, Share same dwr table moudles must be the same 'dwi.load.time.format.by' and same 'dwr.load.time.format.by', Detail:
                   |dwrLoadTimeFormat: ${dwrLoadTimeFormat.toPattern}
                   |cer.dwrLoadTimeDateFormat: ${cer.getMixTransactionManager().dwrLoadTimeDateFormat().toPattern}
                   |dwiLoadTimeFormat: ${dwiLoadTimeFormat.toPattern}
                   |cer.dwiLoadTimeDateFormat: ${cer.getMixTransactionManager().dwiLoadTimeDateFormat().toPattern}
                 """.stripMargin
              )
            }
          }
          cer.addMoudle(m, isM)
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

  def init() = {

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
    Logger.getLogger("org.quartz").setLevel(Level.INFO)

    //    Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.storage.BlockManagerInfo").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.sql.catalyst.parser.CatalystSqlParser").setLevel(Level.WARN)

    initModulesConfig()
    initMC()

    val mySqlJDBCClientV2= new MySqlJDBCClientV2("app", allModulesConfig.getString(s"rdb.url"), allModulesConfig.getString(s"rdb.user"), allModulesConfig.getString(s"rdb.password"))
    RDBConfig.init(mySqlJDBCClientV2)

    //kafka 故障 offset回滚处理
    resetOffset(mySqlJDBCClientV2)

    //参数化克隆模块
    cloneModules()

    //app name
    val conf = initSparkConf()
//    val appName = conf.get("spark.app.name")

    conf.registerKryoClasses(Array(
      classOf[UuidStat],
      classOf[com.mobikok.ssp.data.streaming.util.Logger],
      classOf[org.apache.log4j.Logger],
      classOf[org.apache.spark.sql.Column]
    ))

    val ssc = new StreamingContext(conf, Seconds(allModulesConfig.getInt("spark.conf.streaming.batch.buration")))

    //check task Whether has been launched.
    if(YarnAPPManagerUtil.isAppRunning(appName)){
      if("true".equals(argsConfig.get(ArgsConfig.FORCE_KILL_PREV_REPEATED_APP))) {
        YarnAPPManagerUtil.killApps(appName, false, appId)
      }else {
        throw new RuntimeException(s"This app '$appName' has already submit,forbid to re-submit!")
      }
    }

    initAllModuleInstance() { case (concurrentGroup, moduleName, moduleClass/*, structType*/) =>

      val shufflePartitions = allModulesConfig.getInt("spark.conf.set.spark.sql.shuffle.partitions")
      val runnableModuleNames = runnableModulesConfig.getConfig("modules").root.map(_._1).toArray
      var cer = generateMixMoudlesBatchController(new HiveContext(ssc.sparkContext), shufflePartitions, moduleName, runnableModuleNames)


      val m: Module = Class
        .forName(moduleClass)
        .getConstructor(classOf[Config], argsConfig.getClass, concurrentGroup.getClass, cer.getClass, moduleName.getClass, runnableModuleNames.getClass, ssc.getClass)
        .newInstance(allModulesConfig, argsConfig, concurrentGroup, cer, moduleName, runnableModuleNames, ssc).asInstanceOf[Module]

      val isInitable = cer.isInitable(moduleName)
      val isRunnable = cer.isRunnable(moduleName)
      LOG.warn(s"Module is instanced !! \nmoduleName: $moduleName, \ninitable: $isInitable, \nrunnable: $isRunnable\n")

      if(isInitable){
        m.init
      }

      if(isRunnable) {
        m.start
      	if(hasSparkStreaming){
          m.handler
      	}
        //       m.stop()
      }
    }

    ssc
    //    val hiveContext = new HiveContext(ssc.sparkContext)
    //    val sqlContext = new SQLContext(ssc.sparkContext)
    //    (hiveContext, sqlContext, ssc)
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

  def initMC(): Unit ={
    MC.init(new MessageClient(allModulesConfig.getString("message.client.url")))
  }

  def initModulesConfig(): Unit ={
    var refs: java.util.List[String]= null
    var ms: Config = null
//    val appName = sparkConf.get("spark.app.name")

    try {
      ms = allModulesConfig.getConfig("modules")
    }catch {case e:Exception=>}
    if(ms != null) {
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

          // 给kafka partitions中的topic加上对应的version信息
          var vTps = allModulesConfig
            .getConfigList(s"modules.${vName}.kafka.consumer.partitoins")
            .map{y=>
              var tp = new java.util.HashMap[String, Any]()
              tp.put("topic", versionFeaturesKafkaTopicName(version, y.getString("topic")))
              if(y.hasPath("partition")) tp.put("partition", y.getInt("partition"))
              tp
            }
          allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.kafka.consumer.partitoins", ConfigValueFactory.fromIterable(vTps))

          // 清理
          allModulesConfig = allModulesConfig.withoutPath(s"modules.${name}")
        }

        // 读取kafka服务器，获取tipic对应的partition并补全配置
        var tps = allModulesConfig
          .getConfigList(s"modules.${vName}.kafka.consumer.partitoins")
          .filter{y=> !y.hasPath("partition")}
          .map{y=> y.getString("topic")}
          .map{y=> KafkaOffsetTool.getTopicPartitions(allModulesConfig.getString("kafka.consumer.set.bootstrap.servers"), java.util.Collections.singletonList(y))}
          .flatMap{y=> y}
          .map{y=> y.asTuple}
          .union(allModulesConfig.getConfigList(s"modules.${vName}.kafka.consumer.partitoins")
            .filter{y=> y.hasPath("partition")}
            .map{y=> (y.getString("topic"), y.getInt("partition"))})
          .distinct
          .map{y=>
            var tp = new java.util.HashMap[String, Any]()
            tp.put("topic", y._1)
            tp.put("partition", y._2)
            tp
          }
        allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.kafka.consumer.partitoins", ConfigValueFactory.fromIterable(tps))

        if(argsConfig.has(ArgsConfig.EX)) {
          //拿到用户配置的ex
          val exDims = argsExDimColmunNames(argsConfig).map(_.trim).toSet
          if(allModulesConfig.hasPath(s"modules.${vName}.dwr.groupby.fields")){
            // 排除dwr.groupby.fields中不需要统计的字段
            var fields = allModulesConfig
              .getConfigList(s"modules.${vName}.dwr.groupby.fields")
              .map{y=>
                var filed = new java.util.HashMap[String, String]()
                if(exDims.contains(y.getString("as"))){
                  filed.put("expr", "null")
                }else{
                  filed.put("expr", y.getString("expr"))
                }
                filed.put("as", y.getString("as"))
                filed
              }
            allModulesConfig = allModulesConfig.withValue(s"modules.${vName}.dwr.groupby.fields", ConfigValueFactory.fromIterable(fields))
          }
        }
      }
    }

    try {
      refs = allModulesConfig.getStringList("ref.modules")
    }catch {
      case e:Exception =>
        LOG.warn("No ref modules config ")
    }
    if(refs != null) {
      refs.foreach{x=>

        var f = new File(x)
        if(!f.exists()) {
          throw new AppException(s"The ref modules config file '$x' does not exist")
        }

        val refC = ConfigFactory.parseFile(f)//load(args(0))

        LOG.warn(s"\nApp ref config file ${x}(exists: ${f.exists()}):\n" + refC.root().unwrapped().toString +"\n")

        allModulesConfig = allModulesConfig.withFallback(refC)

        refC.getConfig("modules").root().foreach{y=>
          allModulesConfig = allModulesConfig.withValue(s"modules.${y._1}", y._2)
          //config = config.withValue(s"modules.${y._1}.concurrent.group", refC.getValue("spark.conf.app.name"))
        }

      }
    }

    //reset buration
    try{
      if(argsConfig.has(ArgsConfig.STREAMING_BATCH_BURATION)) {
        LOG.warn("reset duration !")
        if(hasSparkStreaming){
          allModulesConfig = allModulesConfig.withValue("spark.conf.streaming.batch.buration", ConfigValueFactory.fromAnyRef(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION)) )
        }else{

          allModulesConfig.getConfig("modules").root().map { case (moduleName, _) =>
            val cronExpPath = s"modules.$moduleName.dm.handler.cron_exp"
            var cron_exp = ""
            if(allModulesConfig.hasPath(cronExpPath)){
              cron_exp = allModulesConfig.getString(cronExpPath)

            }else{
              cron_exp = "0 0/2 * * * ?"

            }

            val buration = Math.round(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION).toDouble / 60)
            val newConExp = cron_exp.replaceAll("\\/[0-9]", s"\\/${buration}")

            allModulesConfig = allModulesConfig.withValue(cronExpPath, ConfigValueFactory.fromAnyRef(newConExp))

          }
        }

        LOG.warn("reset duration done!")
      }
    }catch {
      case e:Exception=>
        throw new AppException("No batch.buration be configured !!!")
    }

    //reset maxRatePerPartition
    try{
      if(argsConfig.has(ArgsConfig.RATE)) {
        LOG.warn("reset maxRatePerPartition !")

        val partitionNum = argsModuleNames(argsConfig).map { x =>
          allModulesConfig.getList(s"modules.${x}.kafka.consumer.partitoins").size()
        }.sum

        /*val partitionNum = allModulesConfig.getConfig("modules").root().map{x =>
          allModulesConfig.getList(s"modules.${x._1}.kafka.consumer.partitoins").size()
        }.max*/

        val maxRatePerPartition = Integer.valueOf(argsConfig.get(ArgsConfig.RATE))/Integer.valueOf(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION))/partitionNum

        LOG.warn("reset maxRatePerPartition ", "partitionNum ",partitionNum, "maxRatePerPartition ", maxRatePerPartition)
        allModulesConfig = allModulesConfig.withValue("spark.conf.set.spark.streaming.kafka.maxRatePerPartition", ConfigValueFactory.fromAnyRef(maxRatePerPartition))
      }
    }catch {
      case e:Exception=>
        throw new AppException("No maxRatePerPartition can be configured !!!")
    }

    runnableModulesConfig = allModulesConfig

    // 去掉不需要运行的模块配置
    if(argsConfig.has(ArgsConfig.MODULES)) {
      val rms = argsModuleNames(argsConfig).map(_.trim).distinct

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
      if(ms.root().size() == 0) {
        throw new AppException("No any modules be configured !!!")
      }

    } catch {case e:Exception=>
      throw new AppException("No modules be configured !!!")
    }

    LOG.warn("\nApp runnable modules final config content:\n" + runnableModulesConfig.root().unwrapped().toString +"\n")

  }

  def argsExDimColmunNames(argsConfig: ArgsConfig):Array[String] = {
    argsConfig.get(ArgsConfig.EX).split(",")
  }

  def argsModuleNames(argsConfig: ArgsConfig):Array[String] = {
    argsConfig.get(ArgsConfig.MODULES).split(",").map(x=>versionFeaturesModuleName(version, x))
  }

  def versionFeaturesModuleName(version: String, moduleName: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) moduleName else s"${moduleName}_v${version}".trim
  }

  def versionFeaturesTableName(version: String, table: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) table else s"${table}_v${version}".trim
  }

  def versionFeaturesKafkaTopicName(version: String, kafkaTopic: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) kafkaTopic else s"${kafkaTopic}_v${version}".trim
  }

 //hasSparkStreaming
  def hasSparkStreaming(): Boolean = {

    var streamingModuleNum = 0
    allModulesConfig.getConfig("modules").root().map { case (moduleName, _) =>

      streamingModuleNum += allModulesConfig.getConfigList(s"modules.$moduleName.kafka.consumer.partitoins")/*.filter { x =>

//        LOG.warn("hasSparkStreaming",  "topic", x.getString("topic"))
        !"topic_empty".equals(x.getString("topic"))

      }*/.size

//      if(!"topic_empty".equals(config.getString(s"modules.$moduleName.kafka.consumer.partitoins.topic"))){
//        return true
//      }

    }

    LOG.warn("hasSparkStreaming",  "streamingModuleNum", streamingModuleNum)

    streamingModuleNum > 0
  }

  //clone modules
  def cloneModules(): Unit = {

    try{
      if(argsConfig.has(ArgsConfig.CLONE) && "true".equals(argsConfig.get(ArgsConfig.CLONE))){
        LOG.warn("clone module !")

        val clonePrefix = "clone_"

        //替换配置中hive dwi/dwr表名，以及hbase表名
        allModulesConfig.getConfig("modules").root().foreach { case (moduleName, _) =>
          val dwiTPath = s"modules.$moduleName.dwi.table"
          val dwrTPath = s"modules.$moduleName.dwr.table"
          val hbTPath = s"modules.$moduleName.dwi.phoenix.table"

          if(allModulesConfig.hasPath(dwiTPath)){
            val dwiT = s"$clonePrefix${allModulesConfig.getString(dwiTPath)}"
            allModulesConfig = allModulesConfig.withValue(dwiTPath, ConfigValueFactory.fromAnyRef(dwiT))
          }

          if(allModulesConfig.hasPath(dwrTPath)){
            val dwrT = s"$clonePrefix${allModulesConfig.getString(dwrTPath)}"
            allModulesConfig = allModulesConfig.withValue(dwrTPath, ConfigValueFactory.fromAnyRef(dwrT))
          }

          if(allModulesConfig.hasPath(hbTPath)){
            val hbT = s"$clonePrefix${allModulesConfig.getString(hbTPath)}"
            allModulesConfig = allModulesConfig.withValue(hbTPath, ConfigValueFactory.fromAnyRef(hbT))
          }
        }

        //clone所有模块配置
        allModulesConfig.getConfig("modules").root().foreach { case (moduleName, confValue) =>

          allModulesConfig = allModulesConfig.withValue(s"modules.$clonePrefix$moduleName", ConfigValueFactory.fromAnyRef(confValue))

        }

        //过滤原有模块配置
        allModulesConfig.getConfig("modules").root().foreach { case (moduleName, _) =>

          if(!moduleName.contains(clonePrefix)){
            allModulesConfig = allModulesConfig.withoutPath(s"modules.$moduleName")
          }

        }

        LOG.warn("\nApp clone-final config content:\n" + allModulesConfig.root().unwrapped().toString +"\n")
      }

    }catch {
      case e:Exception =>
        e.printStackTrace()
        throw new AppException("clone module failure ")
    }
  }

  //reset kafka offset
  def resetOffset(mySqlJDBCClientV2: MySqlJDBCClientV2): Unit ={

    try{
      if(argsConfig.has(ArgsConfig.KAFKA_OFFSET_ROLLBACK) && "true".equals(argsConfig.get(ArgsConfig.KAFKA_OFFSET_ROLLBACK))) {
        LOG.warn("reset kafka-offset !")

        //delete the module's record from rollbackable_transaction_cookie

        val deleteTransactionSqls = allModulesConfig.getConfig("modules").root().map{case (module, _) =>

          s"""
             |delete from rollbackable_transaction_cookie where module_name = '$module'
           """.stripMargin
        }.toArray

        mySqlJDBCClientV2.executeBatch(deleteTransactionSqls)

        LOG.warn("kafka offset rollback reset", "KAFKA_OFFSET_ROLLBACK_LATEST_HOURS", argsConfig.has(ArgsConfig.KAFKA_OFFSET_ROLLBACK_LATEST_HOURS))
        if(argsConfig.has(ArgsConfig.KAFKA_OFFSET_ROLLBACK_LATEST_HOURS)){
          //recent topic/partition offset
          val rollbackHours = argsConfig.get(ArgsConfig.KAFKA_OFFSET_ROLLBACK_LATEST_HOURS).toDouble

          LOG.warn(s"kafka offset rollback reset offset to $rollbackHours hours ago")

          val topic = "latest_partition_offset_topic"
          val consumer = "latest_partitions_offset_consumer"

          MC.pull(consumer, Array(topic), {ms =>

            //取rollbackHours小时前的一个offset记录
            val latestData = ms
              .filter{m =>
                (CSTTime.now.ms() - m.getUpdateTime.getTime)/1000/60/60 >= rollbackHours
              }
              .reverse
              .take(1)

            LOG.warn("kafka offset rollback","latest updateTime", latestData.map(_.getUpdateTime))
            val updateOffsetSqls = latestData
              .flatMap{m =>
                  OM.toBean(m.getKeyBody, new TypeReference[Array[LatestOffsetRecord]]() {})
              }
              .map{x =>
                s"""
                   |UPDATE offset
                   |SET offset = ${x.untilOffset}
                   |WHERE module_name = '${x.module}' and topic =  '${x.topic}' and partition = '${x.partition}'
                   |
                 """.stripMargin
              }
              .toArray

            LOG.warn("kafka offset rollback", "updateOffsetSqls ", updateOffsetSqls.toList, "size", updateOffsetSqls.length)
            if(updateOffsetSqls.length > 0){
              mySqlJDBCClientV2.executeBatch(updateOffsetSqls, 200)
            }

            false
          })

        }else{
          LOG.warn("kafka offset rollback delete offset !")

          val deleteOffsetSqls = allModulesConfig.getConfig("modules").root().map{case (module, _) =>
            //设置auto.offset.reset 为latest
            allModulesConfig = allModulesConfig.withValue(s"modules.$module.kafka.consumer.set.auto.offset.reset", ConfigValueFactory.fromAnyRef("latest"))

            s"""
               |delete from offset where module_name = '$module'
           """.stripMargin
          }.toArray

          LOG.warn("kafka offset rollback", "deleteOffsetSqls", deleteOffsetSqls.toList)
          //delete record of modules

          if(deleteOffsetSqls.length > 0){
              mySqlJDBCClientV2.executeBatch(deleteOffsetSqls)
            }
        }

//        }
        //去除kafka 回滚参数
        if(argsConfig.has(ArgsConfig.KAFKA_OFFSET_ROLLBACK)){
          argsConfig = argsConfig.drop(ArgsConfig.KAFKA_OFFSET_ROLLBACK)
        }

      }
    }catch {
      case e:Exception=>
        e.printStackTrace()
        throw new AppException("No kafka-offset can be configured !!!")
    }
  }
  
  
  def initAllModuleInstance()(newModule: (String, String, String/*, StructType*/) => Unit) : Unit = {
    // 初始化与当前runnable模块关联同一个dwr表的模块
    allModulesConfig.getConfig("modules").root.map{ case(moduleName, _)=>
        initModuleInstance(moduleName, moduleName, newModule)
    }.toArray

  }

  private def initModuleInstance(concurrentGroup: String, moduleName: String, newModule: (String, String, String/*, StructType*/) => Unit): Unit = {
    val n = moduleName
    val mc = allModulesConfig.getString(s"modules.${n}.class")
    newModule(concurrentGroup, n, mc)
  }


}
//object X{
//  def main (args: Array[String]): Unit = {
//    val refC = ConfigFactory.parseFile(new File("C:\\Users\\Administrator\\IdeaProjects\\datastreaming\\src\\main\\scala\\dw\\fee.conf"))//load(args(0))
//
//   //println(refC.getConfig("modules").withValue("asd", refC.getValue("")))
//  }
//}