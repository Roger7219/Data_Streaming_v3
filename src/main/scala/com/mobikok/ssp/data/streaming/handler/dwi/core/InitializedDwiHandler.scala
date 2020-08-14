package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.module.support.repeats.RepeatsFilter
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager}
import com.mobikok.ssp.data.streaming.util.{MessageClient, ModuleTracer}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * 流统计第一个要调用的Handler，包含两个功能：
  * 1）将原始数据转成成包含b_time、b_date、l_time、repeats等分区字段的DataFrame
  * 2）去重功能
  *
  * Core handler, default configure is {modules.$moduleName.dwi.uuid.enable= true}
  */
class InitializedDwiHandler extends Handler {

  var repeatsFilter: RepeatsFilter = _
  var businessTimeExtractBy: String = _
  var isEnableDwiUuid: Boolean = _ // 是否开启去重功能

  var dwiBTimeFormat:String =_

  def this(repeatsFilter: RepeatsFilter, businessTimeExtractBy: String, isEnableDwiUuid: Boolean, dwiBTimeFormat:String, argsConfig: ArgsConfig) {
    this()
    this.repeatsFilter = repeatsFilter
    this.businessTimeExtractBy = businessTimeExtractBy
    this.isEnableDwiUuid = isEnableDwiUuid
    this.dwiBTimeFormat = dwiBTimeFormat
  }

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)
  }

  override def doHandle(newDwi: DataFrame): DataFrame = {
    LOG.warn("uuid handler dwi schema", newDwi.schema.fieldNames)

    val dwiLTimeExpr = transactionManager.dwiLTime(moduleConfig)

    var uuidDwi: DataFrame = null
    if (isEnableDwiUuid) {
      uuidDwi = repeatsFilter
        .filter(newDwi)
        .alias("dwi")
        .selectExpr(
          s"dwi.*", // uuidFilter返回的DataFrame 包含了repeats、dwi.*以及repeated字段
          s"'$dwiLTimeExpr' as l_time",
          s"cast(to_date($businessTimeExtractBy) as string)  as b_date",
          s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwiBTimeFormat')  as b_time"
         /* , s"'0' as b_version"*/
      )
    } else {
      uuidDwi = newDwi
        .selectExpr(
          s"0 as repeats",
          s"dwi.*",
          s"'N' as repeated",
          s"'$dwiLTimeExpr' as l_time",
          s"cast(to_date($businessTimeExtractBy) as string)  as b_date",
          s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwiBTimeFormat')  as b_time"
          /*, s"'0' as b_version"*/
        )
    }

    LOG.warn("uuid handler after dwi schema", uuidDwi.schema.fieldNames)

    uuidDwi
  }

  override def doCommit(): Unit = {}

  override def doClean(): Unit = {
  }
}
















//object testbloom{
//
//  def main (args: Array[String] ): Unit = {
//    var _bt = "2018-12-12 11:13:44"
//    var bts = CSTTime.neighborTimes(_bt, 1.0, 0)
//    println(bts.toSet)
//
//    var size = 2409300
//    val bf = new BloomFilter(40*size, 16 /*16*/, Hash.MURMUR_HASH)
//    for(i <- 1 to size) {
//      bf.add(new Key(UUID.randomUUID().toString().getBytes))
//    }
//
//    for (j <- 1 to 100) {
//      var c = 0
//      for(i <- 1 to size) {
//        if(bf.membershipTest(new Key(UUID.randomUUID().toString().getBytes))){
//          c = c+1
//        }
//      }
//      println(c)
//    }
//
//
//  }
//}