package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.Uuid
import com.mobikok.ssp.data.streaming.util.OM
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AggUserActiveHandler extends Handler {

  var uuidTable = null.asInstanceOf[String]
//  var dwiTable = null.asInstanceOf[String]

  override def init (moduleName: String, transactionManager:TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, exprStr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, exprStr, as)

    uuidTable = handlerConfig.getString("uuid.table")
  }

//  @volatile var partentTid:String = null
//  @volatile var subIncrTid:Int = 7

  override def handle (newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"AggUserActiveHandler handle starting")
//    LOG.warn("AggUserActiveHandler newDwi.count", newDwi.count())

//    if(partentTid == null) {
      val partentTid = transactionManager.beginTransaction(moduleName)
//    }

    val newUuidDf = newDwi
      .select(concat_ws("^", expr("to_date(createTime)"), col("appId"), col("jarId"), col("imei")) as "uuid")
      .dropDuplicates("uuid")
      .alias("newUuidDf")

    LOG.warn(s"distincted rowkey newUuidDf count(): ${newUuidDf.count()}, take(2)", newUuidDf.take(2))

    val hbaseDf = hbaseClient.getsAsDF(uuidTable,newUuidDf.rdd.map(x => x.getAs[String]("uuid")),classOf[Uuid]).alias("hbaseDf")

    LOG.warn("AggUserActiveHandler hbaseDf count", hbaseDf.count())

    val extDwiDf = newDwi
        .alias("dwi")
        .join(hbaseDf, col( "hbaseDf.uuid") === concat_ws("^", expr("to_date(dwi.createTime)"), col("dwi.appId"), col("dwi.jarId"), col("dwi.imei")) ,"left_outer")
        .select(expr("""if(hbaseDf.uuid is null, "N", "Y") AS dayRepeated"""), col("dwi.*"))

    LOG.warn(s"AggUserActiveHandler joined count: ${extDwiDf.count()}, take(2)", extDwiDf.take(2) )
    //write to hbase

    val dwiP = newUuidDf
      .toJSON
      .rdd
      .map {x =>
        OM.toBean(x, classOf[Uuid])
      }
      .collect()
//    LOG.warn(s"AggUserActiveHandler's data of write to hbase  take(2)", dwiP.take(2))

//    val tid = partentTid + TransactionManager.parentTransactionIdSeparator + (subIncrTid)
//    subIncrTid = subIncrTid + 1

    hbaseClient.putsNonTransaction(uuidTable, dwiP)
    (extDwiDf, Array[TransactionCookie]())

//    val c = hbaseClient.puts(partentTid/*tid*/, uuidTable, dwiP)
//
////    hbaseClient.commit(c)
//
//    (extDwiDf, Array(c))

  }

  override def init (): Unit = {}

  override def commit (cookie: TransactionCookie): Unit = {
//    partentTid = null
//    subIncrTid = 7
    hbaseClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hbaseClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    hbaseClient.clean(cookies:_*)
  }
}