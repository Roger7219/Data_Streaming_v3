package com.mobikok.ssp.data.streaming.handler.dwi.core

import java.util.UUID

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.module.support.uuid.UuidFilter
import com.mobikok.ssp.data.streaming.util.CSTTime
import com.typesafe.config.Config
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.sql.DataFrame

/**
  * Core handler, default configure is {modules.$moduleName.dwi.uuid.enable= true}
  */
class UUIDFilterDwiHandler extends Handler {

  // 持久化，用于clean
//  var uuidDwi: DataFrame = _
  var uuidFilter: UuidFilter = _
  var businessTimeExtractBy: String = _
  var isEnableDwiUuid: Boolean = _

  var dwiBTimeFormat:String =_
//  var argsConfig: ArgsConfig = _
//  var version:String = "0"

  def this(uuidFilter: UuidFilter, businessTimeExtractBy: String, isEnableDwiUuid: Boolean, dwiBTimeFormat:String, argsConfig: ArgsConfig) {
    this()
    this.uuidFilter = uuidFilter
    this.businessTimeExtractBy = businessTimeExtractBy
    this.isEnableDwiUuid = isEnableDwiUuid
    this.dwiBTimeFormat = dwiBTimeFormat
//    this.version = argsConfig.getOrDefault(ArgsConfig.VERSION, ArgsConfig.Value.VERSION_DEFAULT);
  }

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {
    LOG.warn("uuid handler dwi schema", newDwi.schema.fieldNames)

    val dwiLTimeExpr = transactionManager.asInstanceOf[MixTransactionManager].dwiLoadTime(moduleConfig)

    var uuidDwi = null.asInstanceOf[DataFrame]
    if (isEnableDwiUuid) {
      uuidDwi = uuidFilter
        .filter(newDwi)
        .alias("dwi")
        .selectExpr(
          s"dwi.*",
          s"'$dwiLTimeExpr' as l_time",
          s"cast(to_date($businessTimeExtractBy) as string)  as b_date",
          s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwiBTimeFormat')  as b_time",
          s"'0' as b_version"
      )
    } else {
      uuidDwi = newDwi
        .selectExpr(
          s"0 as repeats",
          s"dwi.*",
          s"'N' as repeated",
          s"'$dwiLTimeExpr' as l_time",
          s"cast(to_date($businessTimeExtractBy) as string)  as b_date",
          s"from_unixtime(unix_timestamp($businessTimeExtractBy), '$dwiBTimeFormat')  as b_time",
          s"'0' as b_version"
        )
    }
    LOG.warn("uuid handler after dwi schema", uuidDwi.schema.fieldNames)
//    this.uuidDwi = uuidDwi
//    uuidDwi.persist(StorageLevel.MEMORY_ONLY_SER)
//    uuidDwi.count()
    (uuidDwi, Array())
  }

  override def init(): Unit = {}

  override def commit(cookie: TransactionCookie): Unit = {}

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    new Cleanable()
  }

  override def clean(cookies: TransactionCookie*): Unit = {
//    if (uuidDwi != null) {
//      try {
//        uuidDwi.unpersist()
//      } catch {
//        case e: Exception =>
//      }
//    }
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