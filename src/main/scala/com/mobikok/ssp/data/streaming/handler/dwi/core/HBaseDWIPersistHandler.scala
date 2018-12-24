package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HBaseDWIPersistHandler extends Handler {

  var cookie: TransactionCookie = _

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    isAsynchronous = true
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, globalConfig, expr, as)
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hbaseClient.rollback(cookies:_*)
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    (newDwi, Array())
  }

  override def commit(cookie: TransactionCookie): Unit = {
    hiveClient.commit(this.cookie)
    if (cookie != null) {
      hiveClient.commit(cookie)
    }
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    hbaseClient.clean(cookies:_*)
  }
}
