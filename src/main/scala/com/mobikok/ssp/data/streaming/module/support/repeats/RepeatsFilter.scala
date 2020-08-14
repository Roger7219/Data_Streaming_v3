package com.mobikok.ssp.data.streaming.module.support.repeats

import java.util.Date

import com.mobikok.ssp.data.streaming.util.{CSTTime, Logger, ModuleTracer}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * 计算重复数据的过滤器
  * Created by Administrator on 2018/4/17.
  */
trait RepeatsFilter {

  var LOG: Logger = null

  protected var config: Config = _
  protected var hiveContext: HiveContext = _
  protected var moduleTracer: ModuleTracer = _
  protected var dwiUuidFieldsAlias: String = _
  protected var businessTimeExtractBy: String = _
  protected var shufflePartitions:Integer = _
  protected var dwiTable: String = _

  def init(moduleName: String,
           config: Config,
           hiveContext: HiveContext,
           moduleTracer: ModuleTracer,
           dwiUuidFieldsAlias: String,
           businessTimeExtractBy:String,
           dwiTable: String
          ): Unit = {

    LOG = new Logger(moduleName, getClass, new Date().getTime)
    this.config = config
    this.shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")
    this.hiveContext = hiveContext
    this.moduleTracer = moduleTracer
    this.dwiUuidFieldsAlias = dwiUuidFieldsAlias
    this.businessTimeExtractBy = businessTimeExtractBy
    this.dwiTable = dwiTable
  }

  def filter(dwi: DataFrame): DataFrame

  def dwrNonRepeatedWhere():String
}









//object  xx{
//  def main (args: Array[String]): Unit = {
//    forClusterBTime("2018-12-12 12:12:12", 3)
//  }
//  def forClusterBTime(b_time: String, spanHour: Integer): Unit= {
//
////    val spanHour = 1
////    val b_time = "2018-02-01 09:11:11"
//
//    //		0 2 4 6 8 10 12
//    //间隔毫秒数
//    val interMS = 1000L * 60 * 60 * 24 / (24 / spanHour)
//    System.out.println("interMS: " + interMS)
//
//    val startDayMS = CSTTime.formatter("yyyy-MM-dd 00:00:00").parse(b_time.split(" ")(0) + " 00:00:00").getTime
//
//    System.out.println("startDayMS: " + CSTTime.time(new Date(startDayMS)))
//
//    val currentTimeMS = CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse(b_time).getTime
//    System.out.println("currentTime: " + CSTTime.time(new Date(currentTimeMS)))
//    val nowDayMS = currentTimeMS - startDayMS
//
//    System.out.println("nowDayMS: " + CSTTime.time(new Date(nowDayMS)) + " (" + nowDayMS + ")")
//    //CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse("1970-01-01 00:00:00").getTime() +
//    val pos = Math.ceil(nowDayMS / interMS)
//
//    System.out.println("pos: " + pos)
//    val offsetMS = (1000L * 60 * 60 * (pos - 1) * spanHour).toLong
//
//    System.out.println(CSTTime.time(new Date(startDayMS + offsetMS)))
//
//  }
//}
