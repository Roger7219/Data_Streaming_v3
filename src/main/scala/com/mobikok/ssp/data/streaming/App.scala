package com.mobikok.ssp.data.streaming

import java.io.File
import java.text.SimpleDateFormat
import java.util

import com.mobikok.ssp.data.streaming.config.ArgsConfig
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.module.Module
import com.mobikok.ssp.data.streaming.util.CSTTime
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * 已弃用！用MixApp或OptimizedMixApp代替！！！
  * Created by Administrator  on 2017/6/8.
  */
@deprecated
class App {

}

/**
  * 已弃用！用MixApp或OptimizedMixApp代替！！！
  * Created by Administrator  on 2017/6/8.
  */
@deprecated
object App {

  private[this] val LOG = Logger.getLogger(getClass().getName())
  var config: Config = null
  var argsConfig: ArgsConfig = new ArgsConfig()
  val dataFormat: SimpleDateFormat = CSTTime.formatter("yyyyMMdd-HHmmss")//new SimpleDateFormat("yyyyMMdd-HHmmss")


  def main (args: Array[String]): Unit = {
    try {

      if(args.length > 0) {

        config = ConfigFactory.parseFile(new File(args(0)))//load(args(0))
        LOG.warn(s"Load Config File (exists: ${new File(args(0)).exists()}): " + args(0) + "\n" +config.root().unwrapped().toString + "\n")

        argsConfig = new ArgsConfig().init(args.tail)

        LOG.warn("\nArgsConfig: \n" + argsConfig.toString)

      }else {
        LOG.warn("Load Config File In Classpath")
        config = ConfigFactory.load
      }

      val ssc = init()

      callStartModuleByConf() { case (concurrentGroup, moduleName, moduleClass/*, structType*/) =>

        val m: Module = Class
          .forName(moduleClass)
          .getConstructor(classOf[Config], argsConfig.getClass ,concurrentGroup.getClass, moduleName.getClass,/*structType.getClass,*/ ssc.getClass)
          .newInstance(config, argsConfig, concurrentGroup, moduleName, /*structType,*/ ssc).asInstanceOf[Module]

        m.init
        m.start
        m.handler
  //       m.stop()
      }

  //    ssc.checkpoint(config.getString("spark.conf.streaming.checkpoint"))
      ssc.start
      ssc.awaitTermination
    }catch {case e:Exception=>
      throw new AppException("App run fail !!", e)
    }
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

    Logger.getLogger("org.apache.hadoop.hbase.MetaTableAccessor").setLevel(Level.ERROR)



//    Logger.getLogger("org.apache.spark.ContextCleaner").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark.storage.BlockManagerInfo").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark.sql.catalyst.parser.CatalystSqlParser").setLevel(Level.WARN)

    LOG.info("Spark Conf: \n" + config
      .getConfig("spark.conf.set")
      .entrySet()
      .map { x =>
        x.getKey + "=" + x.getValue.unwrapped.toString
      }.mkString("\n", "\n", "\n"))

    val conf = new SparkConf()
      .setAll(config
        .getConfig("spark.conf.set")
        .entrySet()
        .map { x =>
          (x.getKey, x.getValue.unwrapped.toString)
        })
      .setAppName(config.getString("spark.conf.app.name"))

    conf.registerKryoClasses(Array(
      classOf[UuidStat],
      classOf[com.mobikok.ssp.data.streaming.util.Logger],
      classOf[org.apache.log4j.Logger],
      classOf[org.apache.spark.sql.Column]
    ))

    val ssc = new StreamingContext(conf, Seconds(config.getInt("spark.conf.streaming.batch.buration")))

    ssc
    //    val hiveContext = new HiveContext(ssc.sparkContext)
    //    val sqlContext = new SQLContext(ssc.sparkContext)
    //    (hiveContext, sqlContext, ssc)
  }


  def callStartModuleByConf ()(startModule: (String, String, String/*, StructType*/) => Unit) = {
    var refs: java.util.List[String]= null
    var ms: Config = null

    try {
      ms = config.getConfig("modules")
    }catch {case e:Exception=>}
    if(ms != null) {
      ms.root().foreach{x=>
        config = config.withValue(s"modules.${x._1}.concurrent.group", config.getValue("spark.conf.app.name"))
      }
    }

    try {
      refs = config.getStringList("ref.modules")
    }catch {
      case e:Exception =>
        LOG.warn("No ref modules config !")
    }
    if(refs != null) {
      refs.foreach{x=>

        val refC = ConfigFactory.parseFile(new File(x))//load(args(0))

        LOG.warn(s"\nApp ref config file ${x}(exists: ${new File(x).exists()}):\n" + refC.root().unwrapped().toString +"\n")

        config = config.withFallback(refC)

        refC.getConfig("modules").root().foreach{y=>
          config = config.withValue(s"modules.${y._1}", y._2)
          config = config.withValue(s"modules.${y._1}.concurrent.group", refC.getValue("spark.conf.app.name"))
        }

      }
    }

    LOG.warn("\nApp final config content:\n" + config.root().unwrapped().toString +"\n")

    try {

      ms = config.getConfig("modules")
    }catch {
      case e:Exception=>
        LOG.warn("No modules config !!!")
    }

    if(ms != null) {
      ms.root.foreach { x =>

        callStartModule(ms.getString(s"${x._1}.concurrent.group"), x._1, startModule)
      }
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