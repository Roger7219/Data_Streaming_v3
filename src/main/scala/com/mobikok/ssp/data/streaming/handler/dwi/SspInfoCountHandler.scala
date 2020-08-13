package com.mobikok.ssp.data.streaming.handler.dwi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionRoolbackedCleanable}
import com.mobikok.ssp.data.streaming.util.ModuleTracer
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2018/8/21 0021.
  */

class SspInfoCountHandler extends Handler{
  val tableName = "ssp_info_dwi"

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, moduleTracer)

  }

  def doHandle(newDwi: DataFrame): DataFrame = {

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

    df

  }

  def doCommit (): Unit = {}

  def doClean (): Unit = {}
}
