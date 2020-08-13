package com.mobikok.ssp.data.streaming.config

import com.mobikok.ssp.data.streaming.util.OM

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
  * Created by Administrator on 2017/8/24.
  */
class ArgsConfig {

  private var argsMap: Map[String, String] = Map[String, String]()
//  private var defautArgsMap: Map[String, String] = Map[String, String]((ArgsConfig.VERSION, ArgsConfig.VERSION_DEFAULT))

  def init(args: Array[String]): ArgsConfig = {
    if(args == null || args.length == 0) {
      return this
    }
    argsMap = args
      .mkString(" ")
      .trim
      .replaceAll("[ \t]*=[ \t]", "=")
      .replaceAll("[ \t]*,[ \t]", ",")
      .split(" ")
      .map{ x=>
        x.split("=")(0) ->  x.split("=")(1)
      }.toMap
    this
  }

  def get(configName: String) : String = {
//    argsMap.get(configName).getOrElse(null)
    get(configName, null)
  }
  def get(configName: String, defaultValue: String) : String = {
    argsMap.get(configName).getOrElse(defaultValue)
  }

  /*def update(configName: String, value: String): ArgsConfig = {
    argsMap = argsMap.updated(configName, value)
    this
  }*/

  def drop(configName: String): ArgsConfig = {
    argsMap = argsMap.dropWhile{case (key, _) => key.contains(configName)}
    this
  }


  def has(configName: String): Boolean = {
    argsMap.containsKey(configName)
  }

//  def getElse(configName: String, elseValue: String) : String = {
//    val v = get(configName)
//    if(v == null){
//      elseValue
//    }else {
//      v
//    }
//  }

  override def toString: String = {
    OM.toJOSN(argsMap.asJava)
  }
}

object ArgsConfig{

//  val REBRUSH = "rebrush"
  val MODULES = "modules"
  val STREAMING_BATCH_BURATION = "buration"
  val FORCE_KILL_PREV_REPEATED_APP = "kill"
  val RATE = "rate"
//  val KAFKA_OFFSET_ROLLBACK = "offsetRollback"
//  val KAFKA_OFFSET_ROLLBACK_LATEST_HOURS = "rollbackHours"
//  val CLONE = "clone"
  val VERSION = "version"
  val EX = "ex" //Exclude，排除指定的维度字段
  var TOPIC_VERSION = "topic.version" // 指定kafka topic版本

  val OFFSET = "offset" // earliest or latest
//  var ROLLBACK = "rollback"

  object Value{
//    val REBRUSH_RUNNING = "running"
//    val REBRUSH_DONE = "done"
    val VERSION_DEFAULT = "0"
    val KAFKA_TOPIC_VERSION_DEFAULT = "0"

    val OFFSET_EARLIEST = "earliest"
    val OFFSET_LATEST = "latest"

//    val ROLLBACK_TRUE = "true"
//    val ROLLBACK_FALSE = "false"

  }

  def main (args: Array[String]): Unit = {
//    d("xx","ww","r")

    val ss=  new Array[String](0)
      .mkString(" ")
      .trim
      .replaceAll("[ \t]*=[ \t]", "=")
      .replaceAll("[ \t]*,[ \t]", ",")
      .split(" ")
      .map{ x=>
        x.split("=")(0) ->  x.split("=")(1)
      }.toMap
    println(OM.toJOSN( ss ))
  }

  def d(N:String, n2:String*): Unit = {
    print(N+:n2)

  }
}
