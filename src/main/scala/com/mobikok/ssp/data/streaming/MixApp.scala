package com.mobikok.ssp.data.streaming

import java.io.File
import java.text.SimpleDateFormat

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.OptimizedMixApp.{allModulesConfig, argsConfig}
import com.mobikok.ssp.data.streaming.client.MixTransactionManager
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.module.Module
import com.mobikok.ssp.data.streaming.module.support.{AlwaysTransactionalStrategy, MixModulesBatchController, OptimizedTransactionalStrategy}
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
class MixApp {

}

object MixApp {

  private[this] val LOG = Logger.getLogger(getClass().getName())
  var config: Config = null
  var argsConfig: ArgsConfig = new ArgsConfig()
  val dataFormat: SimpleDateFormat = CSTTime.formatter("yyyyMMdd-HHmmss") //new SimpleDateFormat("yyyyMMdd-HHmmss")

  var dwiLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:00:00")
  var dwrLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd 00:00:00")

  def main (args: Array[String]): Unit = {
    try {

      if(args.length > 0) {
        var f = new File(args(0))
        if(!f.exists()) {
          throw new AppException(s"The modules config file '${args(0)}' does not exist")
        }

        config = ConfigFactory.parseFile(f)//load(args(0))

        LOG.warn(s"Load Config File (exists: ${f.exists()}): " + args(0) + "\nsetting plain:" +config.root().unwrapped().toString + s"\nargs config plain: ${java.util.Arrays.deepToString(args.tail.asInstanceOf[Array[Object]])}")

        argsConfig = new ArgsConfig().init(args.tail)

        LOG.warn("\nParsed ArgsConfig: \n" + argsConfig.toString)

      }else {
        LOG.warn("Load Config File In Classpath")
        config = ConfigFactory.load
      }

      val ssc = init()

      //    ssc.checkpoint(config.getString("spark.conf.streaming.checkpoint"))
      ssc.start
      ssc.awaitTermination
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
        ms = config.getConfig("modules")
      }catch {case e:Exception=>}

      if(ms != null) {
        ms.root().foreach{x=>
          var t:String = null
          val m = x._1
          var isM = false
          try {
            if(config.getBoolean(s"modules.${m}.dwr.enable")) {
              t = config.getString(s"modules.${m}.dwr.table")
            }
          }catch {case e:Exception=>
            LOG.warn("Get the dwr table name failed, Caused by " + e.getMessage)
          }
          if(t == null) {
            t = s"non_sharing_dwr_table_module_$m"
          }
          try{
            isM = config.getBoolean(s"modules.$m.master")
          }catch {case e:Exception=>}

          //---------------------------------- Generate l_time DateFormat START ---------------------------------
          try {
            dwiLoadTimeFormat = DateFormatUtil.CST(config.getString(s"modules.$m.dwi.load.time.format.by"))
          }catch { case _: Exception =>}
          if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
            dwiLoadTimeFormat = DateFormatUtil.CST(dwiLoadTimeFormat.toPattern.substring(0, 17) + "01")
          }

          try {
            dwrLoadTimeFormat = DateFormatUtil.CST(config.getString(s"modules.$m.dwr.load.time.format.by"))
          }catch {case _:Exception => }
          if(ArgsConfig.Value.REBRUSH_RUNNING.equals(argsConfig.get(ArgsConfig.REBRUSH))){
            dwrLoadTimeFormat = CSTTime.formatter(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")//new SimpleDateFormat(dwrLoadTimeFormat.toPattern.substring(0, 17) + "01")
          }
          //---------------------------------- Generate l_time DateFormat END ---------------------------------

          var cer = shareTableModulesControllerMap.get(t)
          if(cer == null) {
            cer = new MixModulesBatchController(config, runnableModuleNames ,t, new MixTransactionManager(config, new AlwaysTransactionalStrategy(dwiLoadTimeFormat, dwrLoadTimeFormat)), hiveContext, shufflePartitions)
            shareTableModulesControllerMap.put(t, cer)
          }else {
            if(!dwrLoadTimeFormat.toPattern.equals(cer.getMixTransactionManager().dwrLoadTimeDateFormat().toPattern) || !dwiLoadTimeFormat.toPattern.equals(cer.getMixTransactionManager().dwiLoadTimeDateFormat().toPattern)) {
              throw new AppException(s"Moudle '$m' config is wrong, Share same dwr table moudles must be the same 'dwi.load.time.format.by' and same 'dwr.load.time.format.by' ")
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
    return null
  }

  def init () = {

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

    //    Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.storage.BlockManagerInfo").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark.sql.catalyst.parser.CatalystSqlParser").setLevel(Level.WARN)

    initModulesConfig()
    initMC()

    val mySqlJDBCClientV2= new MySqlJDBCClientV2("app", config.getString(s"rdb.url"), config.getString(s"rdb.user"), config.getString(s"rdb.password"))
    RDBConfig.init(mySqlJDBCClientV2)

    //app name
    val conf = initSparkConf()
    val appName = conf.get("spark.app.name")

    //check task Whether has been launched.
    if(YarnAPPManagerUtil.isAppRunning(appName)){
      if("true".equals(argsConfig.get(ArgsConfig.FORCE_KILL_PREV_REPEATED_APP))) {
        YarnAPPManagerUtil.killApps(appName, true, conf.getAppId);//.killAppsExcludeSelf(appName)
      }else {
        throw new RuntimeException(s"This app '$appName' has already submit,forbid to re-submit!")
      }
    }

    conf.registerKryoClasses(Array(
      classOf[UuidStat],
      classOf[com.mobikok.ssp.data.streaming.util.Logger],
      classOf[org.apache.log4j.Logger],
      classOf[org.apache.spark.sql.Column]
    ))

    val ssc = new StreamingContext(conf, Seconds(config.getInt("spark.conf.streaming.batch.buration")))

    callStartModuleByConf() { case (concurrentGroup, moduleName, moduleClass/*, structType*/) =>

      val shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")
      val runnableModuleNames = config.getConfig("modules").root.map(_._1).toArray
      var cer = generateMixMoudlesBatchController(new HiveContext(ssc.sparkContext), shufflePartitions, moduleName, runnableModuleNames)

      val m: Module = Class
        .forName(moduleClass)
        .getConstructor(classOf[Config], argsConfig.getClass ,concurrentGroup.getClass,cer.getClass, moduleName.getClass, runnableModuleNames.getClass, ssc.getClass)
        .newInstance(config, argsConfig, concurrentGroup, cer, moduleName, runnableModuleNames, ssc).asInstanceOf[Module]

      val isInitable = cer.isInitable(moduleName)
      val isRunnable = cer.isRunnable(moduleName)
      LOG.warn(s"The module is starting !! \nmoduleName: $moduleName, \ninitable: $isInitable, \nrunnable: $isRunnable\n")

      if(isInitable){
        m.init
      }

      if(isRunnable) {
        m.start
        m.handler
        //       m.stop()
      }
    }

    ssc
    //    val hiveContext = new HiveContext(ssc.sparkContext)
    //    val sqlContext = new SQLContext(ssc.sparkContext)
    //    (hiveContext, sqlContext, ssc)
  }

  def initSparkConf (): SparkConf ={

    //加载获取spark-submit启动命令中参数配置
    val conf = new SparkConf(true)

    // 获取spark-submit启动命令中的--name参数指定的
    val cmdAppName = conf.get("spark.app.name").trim
    // 一律用spark-submit启动命令中的--name参数值作为app name
    config = config.withValue("spark.conf.set.spark.app.name", ConfigValueFactory.fromAnyRef(cmdAppName))

    LOG.info("Final Spark Conf: \n" + config
      .getConfig("spark.conf.set")
      .entrySet()
      .map { x =>
        x.getKey + "=" + x.getValue.unwrapped.toString
      }.mkString("\n", "\n", "\n")
    )

    conf.setAll(config
      .getConfig("spark.conf.set")
      .entrySet()
      .map { x =>
        (x.getKey, x.getValue.unwrapped.toString)
      })

    if(getClass.getName.equals(cmdAppName)) {
      throw new AppException(s"Spark app name cannot be a main class name '$cmdAppName' ")
    }
    if(StringUtil.isEmpty(cmdAppName)) {
      throw new AppException("Spark app name not specified !!")
    }

//    // 最终还是采用spark-submit启动命令中的--name参数值
//    conf.setAppName(cmdAppName)

    conf
  }

  def initMC(): Unit ={
    MC.init(new MessageClient(config.getString("message.client.url")))
  }

  def initModulesConfig(): Unit ={
    var refs: java.util.List[String]= null
    var ms: Config = null

    try {
      ms = config.getConfig("modules")
    }catch {case e:Exception=>}
    if(ms != null) {
      ms.root().foreach{x=>
        //config = config.withValue(s"modules.${x._1}.concurrent.group", config.getValue("spark.conf.app.name"))
      }
    }

    try {
      refs = config.getStringList("ref.modules")
    }catch {
      case e:Exception =>
        LOG.warn("No ref modules config")
    }
    if(refs != null) {
      refs.foreach{x=>

        var f = new File(x)
        if(!f.exists()) {
          throw new AppException(s"The ref modules config file '$x' does not exist")
        }

        val refC = ConfigFactory.parseFile(f)//load(args(0))

        LOG.warn(s"\nApp ref config file ${x}(exists: ${f.exists()}):\n" + refC.root().unwrapped().toString +"\n")

        config = config.withFallback(refC)

        refC.getConfig("modules").root().foreach{y=>
          config = config.withValue(s"modules.${y._1}", y._2)
          //config = config.withValue(s"modules.${y._1}.concurrent.group", refC.getValue("spark.conf.app.name"))
        }

      }
    }


    if(argsConfig.has(ArgsConfig.MODULES)) {
      val rms = argsConfig.get(ArgsConfig.MODULES).split(",").map(_.trim).distinct

      config.getConfig("modules").root().foreach{x=>
        if(!rms.contains(x._1)) {
          config = config.withoutPath(s"modules.${x._1}")
        }
      }

      rms.foreach {x =>
        try{
          //验证模块是否配置
          config.getConfig(s"modules.${x}")
        }catch {case e:Throwable=>
          throw new AppException(s"Get module '${x}' config fail", e)
        }
      }
    }

    //reset duration
    try{
      if(argsConfig.has(ArgsConfig.STREAMING_BATCH_BURATION)) {
        LOG.warn("reset duration !")
        config = config.withValue("spark.conf.streaming.batch.buration", ConfigValueFactory.fromAnyRef(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION)) )
      }
    }catch {
      case e:Exception=>
        throw new AppException("No batch.buration be configured !!!")
    }


    try {
      ms = config.getConfig("modules")
      if(ms.root().size() == 0) {
        throw new AppException("No any modules be configured !!!")
      }

      config = config.withValue("spark.conf.set.spark.streaming.concurrentJobs", ConfigValueFactory.fromAnyRef(config.getConfig("modules").root().size()))

    } catch {case e:Exception=>
      throw new AppException("No modules be configured !!!")
    }

    //reset maxRatePerPartition
    try{
      if(argsConfig.has(ArgsConfig.RATE)) {
        LOG.warn("reset maxRatePerPartition !")

        val partitionNum = argsConfig.get(ArgsConfig.MODULES).trim.split(",").map { x =>
          config.getList(s"modules.${x}.kafka.consumer.partitions").size()
        }.sum

        val maxRatePerPartition = Integer.valueOf(argsConfig.get(ArgsConfig.RATE))/Integer.valueOf(argsConfig.get(ArgsConfig.STREAMING_BATCH_BURATION))/partitionNum

        config = config.withValue("spark.conf.set.spark.streaming.kafka.maxRatePerPartition", ConfigValueFactory.fromAnyRef(maxRatePerPartition))
      }
    }catch {
      case e:Exception=>
        throw new AppException("No maxRatePerPartition can be configured !!!")
    }

    LOG.warn("\nApp final config content:\n" + config.root().unwrapped().toString +"\n")

  }

  def callStartModuleByConf ()(startModule: (String, String, String/*, StructType*/) => Unit) = {

    config.getConfig("modules").root.foreach { x =>
      callStartModule(x._1, x._1, startModule)
    }

  }

  private def callStartModule(concurrentGroup: String, moduleName: String, startModule: (String, String, String/*, StructType*/) => Unit) = {
    val n = moduleName
    val mc = config.getString(s"modules.${n}.class")
    startModule(concurrentGroup, n, mc)
  }


}
//object X{
//  def main (args: Array[String]): Unit = {
//    val refC = ConfigFactory.parseFile(new File("C:\\Users\\Administrator\\IdeaProjects\\datastreaming\\src\\main\\scala\\dw\\fee.conf"))//load(args(0))
//
//   //println(refC.getConfig("modules").withValue("asd", refC.getValue("")))
//  }
//}