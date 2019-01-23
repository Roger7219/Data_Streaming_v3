package com.mobikok.ssp.data.streaming.handler.dm.online

import java.util

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

class ClickHouseDMPersistMonthHandler extends Handler with Transactional {

//  val COOKIE_KIND_DWR_CLICKHOUSE_T = "dwrClickHouseT"

  var hiveTable: String = _
  var cookie: TransactionCookie = _
  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()
//  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  override def init(): Unit = {}

  override def init(moduleName: String, transactionManager: TransactionManager, clickHouseClient: ClickHouseClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config, globalConfig: Config): Unit = {
    super.init(moduleName, transactionManager, clickHouseClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig, globalConfig)

    hiveTable = handlerConfig.getString("hive.table")
    isAsynchronous = true
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    clickHouseClient.rollback(cookies:_*)
  }

  // handle 替代之前的overwriteUnionSum
  override def doHandle(persistenceData: DataFrame): Unit = {

//    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwr.partition.fields") // l_time, b_date, b_time
    val partitionFields = Array("l_time", "b_date", "b_time")

    val fields = persistenceData.schema.fieldNames.map{ field =>
      if ("l_time".eq(field)) {
        "date_format(l_time, 'yyyy-MM-01 00:00:00') as l_time"
      } else if ("b_date".eq(field)) {
        "date_format(b_date, 'yyyy-MM-01') as b_date"
      } else if ("b_time".eq(field)) {
        "date_format(b_time, 'yyyy-MM-01 00:00:00') as b_time"
      } else {
        field.toLowerCase()
      }
    }

    var aggFields: Array[String] = null
    try {
      aggFields = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map { c =>
        c.getString("as").toLowerCase()
      }.toArray
    } catch {
      case _: Exception =>
        aggFields = Array(
          "requestcount",
          "sendcount",
          "showcount",
          "clickcount",
          "feereportcount",
          "feesendcount",
          "feereportprice",
          "feesendprice",
          "cpcbidprice",
          "cpmbidprice",
          "conversion",
          "allconversion",
          "revenue",
          "realrevenue",
          "feecpctimes",
          "feecpmtimes",
          "feecpatimes",
          "feecpasendtimes",
          "feecpcreportprice",
          "feecpmreportprice",
          "feecpareportprice",
          "feecpcsendprice",
          "feecpmsendprice",
          "feecpasendprice",
          "winprice",
          "winnotices",
          "newcount",
          "activecount"
        )
    }


    val groupByFields = (fields.toSet -- aggFields.toSet).toArray

    // 做聚合，降低精度，减少数据量
    val updatedData = persistenceData
        .groupBy(groupByFields.head, groupByFields.tail:_*)
        .agg(sum(aggFields.head).as(aggFields.head), aggFields.tail.map{ field => sum(field).as(field)}:_*)
        .selectExpr(fields: _*)

    cookie = clickHouseClient.overwriteUnionSum (
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      hiveTable,
      handlerConfig.getString(s"clickhouse.table"),
      updatedData,
      partitionFields.head,
      partitionFields.tail:_*
    )

    batchTransactionCookiesCache.add(cookie)
  }

  override def commit(c: TransactionCookie): Unit = {
    clickHouseClient.commit(cookie)
  }


  override def clean(cookies: TransactionCookie*): Unit = {
    var result = Array[TransactionCookie]()

    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
    if (mixTransactionManager.needTransactionalAction()) {
      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
      batchTransactionCookiesCache.removeAll(needCleans)
      result = needCleans.toArray
    }
    clickHouseClient.clean(result:_*)
  }

}
