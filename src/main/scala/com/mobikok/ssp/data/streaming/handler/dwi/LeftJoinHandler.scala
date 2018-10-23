package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class LeftJoinHandler extends Handler {

  //  table = "app", on = "app.id  = dwi.appid", select = "app.publisherId"
  var dwiLeftJoin: List[(String, String, String)] = _

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, expr, as)

    dwiLeftJoin = handlerConfig.getConfigList("join.left").map{ x =>
      (x.getString("table"), x.getString("on"), x.getString("select"))
    }.toList
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {
    var handlerDwi = newDwi
    if (dwiLeftJoin != null) {
      dwiLeftJoin.foreach{ x =>
        val table = hiveContext.read.table(x._1).alias(x._1)
        handlerDwi = handlerDwi.join(table, expr(x._2), "left_outer").selectExpr()
      }
    }
    (handlerDwi, Array())
  }

  override def init(): Unit = {}

  override def commit(cookie: TransactionCookie): Unit = {}

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    new Cleanable()
  }

  override def clean(cookies: TransactionCookie*): Unit = {}
}
