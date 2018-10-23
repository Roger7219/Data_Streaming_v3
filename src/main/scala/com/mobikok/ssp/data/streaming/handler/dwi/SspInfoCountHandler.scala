package com.mobikok.ssp.data.streaming.handler.dwi
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2018/8/21 0021.
  */

class SspInfoCountHandler extends Handler{
  val tableName = "ssp_info_dwi"

  override def init (moduleName: String, transactionManager:TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, exprStr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, exprStr, as)

  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn("SspInfoCountHandler start")
    newDwi
      .selectExpr("appId", "imei", "info", "event", "createTime")
      .dropDuplicates("imei")
      .createOrReplaceTempView("newDwiT")

    val df = sql(
      s"""
         |select
         |  appid,
         |  imei,
         |  packageName,
         |  createTime
         |from newDwiT
         |LATERAL VIEW explode(split(info, ',')) tbl1 as packageName
         |where event = '{packageNameList}'
         |
       """.stripMargin)

    df.persist(StorageLevel.MEMORY_ONLY)

    LOG.warn("SspInfoCountHandler", "df count", df.collect().size, "df take 4", df.toJSON.take(4))


    /*df
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .format("orc")
      .insertInto(tableName)*/

    df.unpersist(true)

    LOG.warn("SspInfoCountHandler end")

    (df, Array())

  }

  override def init (): Unit = {}

  override def commit (cookie: TransactionCookie): Unit = {
    hbaseClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hbaseClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    hbaseClient.clean(cookies:_*)
  }
}
