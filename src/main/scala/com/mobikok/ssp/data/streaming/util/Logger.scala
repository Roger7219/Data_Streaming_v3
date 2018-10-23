package com.mobikok.ssp.data.streaming.util

import java.util
import java.util.Date

import com.mobikok.message.Message
import com.mobikok.ssp.data.streaming.config.RDBConfig
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

/**
  * Created by kairenlo on 2017/6/25.
  */
class Logger (moduleName: String, className: String, startTime: Long) extends Serializable {
  val LOGGER = Logger.getLogger(className)
  private var logLastTime = startTime

  def this(clazz: Class[_]) {
    this("", clazz.getName, System.currentTimeMillis())
  }
  def this(className: String) {
    this("", className, System.currentTimeMillis())
  }

  def this(className: String, startTime: Long){
      this("", className, startTime)
  }

  def trace (title: String): Unit = {
    LOGGER.trace(logString(title, ""))
  }
  def trace (title: String, value: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(value)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any ): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}"))
  }
  def trace (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any, key6: String, value6: =>Any): Unit = {
    LOGGER.trace(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}\n${objectToString(key6)}: ${objectToString(value6)}"))
  }

  def error (title: String, e: Throwable): Unit = {
    LOGGER.error(title, e)
  }

  def info (title: String): Unit = {
    LOGGER.info(logString(title, ""))
  }
  def info (title: String, value: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(value)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any ): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}"))
  }
  def info (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any, key6: String, value6: =>Any): Unit = {
    LOGGER.info(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}\n${objectToString(key6)}: ${objectToString(value6)}"))
  }

  def warnForJava (title: String, value: Any): Unit = {
    LOGGER.warn(logString(title, value))
  }

  private var lastLogSwicthStatus = true
  private var lastLogSwicthStatusTime:Long = 0
  private var LOGGING_SIGN = classOf[Logger].getName +".needLog"
  private var LAST_LOG_SWICTH_STATUS_CACHE_TIME_MS = 1000*60*1 //缓存上一次读取的日志开关值,缓存时间,1分钟

  def needLog(): Boolean ={

    var needReadLogSwicthStatus = true
    val trace = ExceptionUtils.getStackTrace(new Exception())
//    println(new Date() + " 0000 trace" +trace)

//    if(new Date().getTime - lastReadLogSwicthStatusTime > 1000*15) {
//      needReadLogSwicthStatus = true
//    }
//    needReadLogSwicthStatus = lastReadLogSwicthStatus

    if(System.currentTimeMillis() - lastLogSwicthStatusTime > LAST_LOG_SWICTH_STATUS_CACHE_TIME_MS) {
      needReadLogSwicthStatus = true
    }else {
      needReadLogSwicthStatus = false
    }

    if(trace.indexOf(LOGGING_SIGN, trace.indexOf(LOGGING_SIGN) + LOGGING_SIGN.length) > 0){
      needReadLogSwicthStatus = false
    }

    if(needReadLogSwicthStatus) {
      if("false".equals(RDBConfig.readConfig(RDBConfig.LOG_SWICTH_STATUS)) ) {
        lastLogSwicthStatus =  false
        lastLogSwicthStatusTime = System.currentTimeMillis()
        LOGGER.setLevel(Level.ERROR)
      }else {
        lastLogSwicthStatus = true
        lastLogSwicthStatusTime = System.currentTimeMillis()
      }
    }
//    println(new Date() + " 0000 lastReadLogSwicthStatus" +lastReadLogSwicthStatus)
    return true//lastLogSwicthStatus

  }

  def objectToString(body: =>Any): String = {
    val v = body
    var str = ""
    if (v.isInstanceOf[Number] || v.isInstanceOf[String]) {
      str = v.toString
    }else if(v.isInstanceOf[Array[Object]]) {
      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
    }else if (v.isInstanceOf[Number] || v.isInstanceOf[String]) {
      str = v.toString
    } else if(v.isInstanceOf[Throwable]) {
      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Throwable])
    } else if(v != null && v.getClass.getName.startsWith("scala.collection")) {
      str = v.toString
    } else {
      try{
        str = OM.toJOSN(v)
      }catch {case e:Throwable=>
        str = String.valueOf(v)
      }
    }
    str
  }

  def warn (title: String): Unit = {
    LOGGER.warn(logString(title, ""))
  }
  def warn (title: String, value: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(value)}"))
  }
  def warn (title: String, key1: =>Any, value1: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}"))
  }
  def warn (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any ): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}"))
  }
  def warn (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}"))
  }
  def warn (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}"))
  }
  def warn (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}"))
  }

  def warn (title: String, key1: =>Any, value1: =>Any, key2: String, value2: =>Any, key3: String, value3: =>Any, key4: String, value4: =>Any, key5: String, value5: =>Any, key6: String, value6: =>Any): Unit = {
    LOGGER.warn(logString(title, s"${objectToString(key1)}: ${objectToString(value1)}\n${objectToString(key2)}: ${objectToString(value2)}\n${objectToString(key3)}: ${objectToString(value3)}\n${objectToString(key4)}: ${objectToString(value4)}\n${objectToString(key5)}: ${objectToString(value5)}\n${objectToString(key6)}: ${objectToString(value6)}"))
  }

  def logString(title: String, value: =>Any): String ={
    val t = new Date().getTime

    //    if(!needLog()){
    //
    //      logLastTime = t
    //      return
    //    }
    var str = objectToString(value)
    //    val v = value
    //
    //    var str = ""
    //    if (v.isInstanceOf[Number] || v.isInstanceOf[String]) {
    //      str = v.toString
    //    } else if (v.isInstanceOf[Array[Object]]) {
    //      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
    //    } else if (v.isInstanceOf[Array[String]]) {
    //      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
    //    } else if (v.isInstanceOf[Exception]) {
    //      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Exception])
    //    } else if(v.isInstanceOf[Throwable])  {
    //      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Throwable])
    //    } else {
    //      try{
    //        str = OM.toJOSN(v)
    //      }catch {case e:Exception=>
    //        str = v.toString
    //      }
    //    }

    val tit = if(moduleName != null && !"".equals(moduleName.trim)) s"[${Thread.currentThread().getId}] $moduleName - $title" else s"[${Thread.currentThread().getId}] $title"

    val c = 100
    val len = (c - tit.length) / 2
    val _s = new StringBuilder
    for (i <- 0 until len) {
      _s.append('-')
    }
    val s = new StringBuffer()
      .append(_s)
      .append(' ')
      .append(tit)
      .append(' ')
      .append(_s)

    logLastTime = t

    s"""
       |$s
       |${str}
       |
       |Using time: ${(t - logLastTime) / 1000D}s
       |
       | """.stripMargin
  }

//  @Deprecated
//  def warn (title: String, value: =>Any): Unit = {
//
//    val t = new Date().getTime
//
////    if(!needLog()){
////
////      logLastTime = t
////      return
////    }
//    var str = objectToString(value)
////    val v = value
////
////    var str = ""
////    if (v.isInstanceOf[Number] || v.isInstanceOf[String]) {
////      str = v.toString
////    } else if (v.isInstanceOf[Array[Object]]) {
////      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
////    } else if (v.isInstanceOf[Array[String]]) {
////      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
////    } else if (v.isInstanceOf[Exception]) {
////      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Exception])
////    } else if(v.isInstanceOf[Throwable])  {
////      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Throwable])
////    } else {
////      try{
////        str = OM.toJOSN(v)
////      }catch {case e:Exception=>
////        str = v.toString
////      }
////    }
//
//    val tit = if(moduleName != null && !"".equals(moduleName.trim)) s"[${Thread.currentThread().getId}] $moduleName - $title" else s"[${Thread.currentThread().getId}] $title"
//
//    val c = 100
//    val len = (c - tit.length) / 2
//    val _s = new StringBuilder
//    for (i <- 0 until len) {
//      _s.append('-')
//    }
//    val s = new StringBuffer()
//      .append(_s)
//      .append(' ')
//      .append(tit)
//      .append(' ')
//      .append(_s)
//
//    LOGGER.warn(
//      s"""
//         |$s
//         |${str}
//         |
//         |Using time: ${(t - logLastTime) / 1000D}s
//         |
//         | """.stripMargin)
//    logLastTime = t
//  }
}
//object x{
//  def main (args: Array[String]): Unit = {
//    var LOGGING_SIGN = classOf[Logger].getName +".needLog"
//    val trace ="java.lang.Exception\n\tat com.mobikok.ssp.data.streaming.util.Logger.needLog(Logger.scala:56)\n\tat com.mobikok.ssp.data.streaming.util.Logger.warn(Logger.scala:83)\n\tat com.mobikok.ssp.data.streaming.client.KafkaClient.createDirectStream(KafkaClient.scala:436)\n\tat com.mobikok.ssp.data.streaming.module.MixModule.handler(MixModule.scala:527)\n\tat com.mobikok.ssp.data.streaming.MixApp$$anonfun$initHeartbeat$2.apply(MixApp.scala:200)\n\tat com.mobikok.ssp.data.streaming.MixApp$$anonfun$initHeartbeat$2.apply(MixApp.scala:188)\n\tat com.mobikok.ssp.data.streaming.MixApp$.com$mobikok$ssp$data$streaming$MixApp$$callStartModule(MixApp.scala:306)\n\tat com.mobikok.ssp.data.streaming.MixApp$$anonfun$callStartModuleByConf$1.apply(MixApp.scala:298)\n\tat com.mobikok.ssp.data.streaming.MixApp$$anonfun$callStartModuleByConf$1.apply(MixApp.scala:297)\n\tat scala.collection.Iterator$class.foreach(Iterator.scala:893)\n\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1336)\n\tat scala.collection.IterableLike$class.foreach(IterableLike.scala:72)\n\tat scala.collection.AbstractIterable.foreach(Iterable.scala:54)\n\tat com.mobikok.ssp.data.streaming.MixApp$.callStartModuleByConf(MixApp.scala:297)\n\tat com.mobikok.ssp.data.streaming.MixApp$.initHeartbeat(MixApp.scala:188)\n\tat com.mobikok.ssp.data.streaming.MixApp$.main(MixApp.scala:58)\n\tat com.mobikok.ssp.data.streaming.MixApp.main(MixApp.scala)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.lang.reflect.Method.invoke(Method.java:498)\n\tat org.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:637)"
//    var needReadLogSwicthStatus =true
//    if(trace.indexOf(LOGGING_SIGN, trace.indexOf(LOGGING_SIGN) + LOGGING_SIGN.length) > 0 ){
//      needReadLogSwicthStatus = false
//    }
//    println(needReadLogSwicthStatus)
//  }
//}
//
//object xx{
//  def main(args: Array[String]): Unit = {
//   var str = null.asInstanceOf[String]
//    val v =List(new Message()).toArray
//    if (v.isInstanceOf[Number] || v.isInstanceOf[String]) {
//      println("x")
//      str = v.toString
//    } else if (v.isInstanceOf[Array[Object]]) {
//      println("b")
//      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
//    } else if (v.isInstanceOf[Array[String]]) {
//
//      str = util.Arrays.deepToString(v.asInstanceOf[Array[Object]])
//    } else if (v.isInstanceOf[Exception]) {
//      str = ExceptionUtils.getStackTrace(v.asInstanceOf[Exception])
//    } else {
//      str = OM.toJOSN(v)
//    }
//
//    print( util.Arrays.deepToString(Array(new Message())));
// //   print( new Message().toString);
//
//  }
//}
