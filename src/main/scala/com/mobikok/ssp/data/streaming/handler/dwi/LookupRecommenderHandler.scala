package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{SspTrafficDWI, Uuid}
import com.mobikok.ssp.data.streaming.util.OM
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class LookupRecommenderHandler extends Handler {

  var sspSendDwiTable = "SSP_SEND_DWI_PHOENIX_V2"

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hbaseClient.rollback(cookies:_*)
  }

  override def handle (newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"AggUserActiveHandler handle starting")
    var hc = hbaseClient.asInstanceOf[HBaseMultiSubTableClient]


    var feeDwi = newDwi.as("feeDwi").drop("raterId", "raterType")//.as("newDwi").selectExpr( "null as raterId", "null as raterType", "newDwi.*")

    var sendDwi = hc.getsMultiSubTableAsDF(
      sspSendDwiTable,
      newDwi.rdd.map(_.getAs[String]("clickId")).collect(),
      classOf[SspTrafficDWI]
    ).alias("sendDwi")

    feeDwi = feeDwi
      .join(sendDwi, expr("feeDwi.clickId = sendDwi.clickId"), "left_outer")
      .selectExpr("sendDwi.raterId", "sendDwi.raterType", "feeDwi.*")

    (feeDwi, Array[TransactionCookie]())
  }

  override def init (): Unit = {}

  override def commit (cookie: TransactionCookie): Unit = {
    hbaseClient.commit(cookie)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    hbaseClient.clean(cookies:_*)
  }
}