package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * Core service, configure key is `dwr.acc.day.enable`
  */
class HiveDWRPersistDayHandler extends Handler with Persistence {

//  val COOKIE_KIND_DWR_ACC_DAY_T = "dwrAccDayT"

  var table: String = _
  var cookie: TransactionCookie = _

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())
  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()


  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, expr: String, as: String): Unit = {
    // 默认为true 遵从配置设置
    isAsynchronous = true
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, expr, as)
//    table = handlerConfig.getString("table")
    // 默认加速天表名为原表明加_accday后缀
    table = s"${globalConfig.getString(s"modules.$moduleName.dwr.table")}_accday"
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {
    val aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList

    val unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    val overwriteAggFields = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => if(x.hasPath("overwrite") && x.getBoolean("overwrite")) x.getString("as") else null
    }.filter(_ != null).toSet

    val groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

//    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwr.partition.fields")
    val partitionFields = Array("l_time", "b_date", "b_time", "b_version")

    val dwrFields = persistenceDwr.schema.fieldNames
    val overwriteFields: java.util.Map[String, String] = new util.HashMap[String, String]()
    // default overwrite values to null
    overwriteFields.put("operatingSystem", "null")
    overwriteFields.put("systemLanguage", "null")
    overwriteFields.put("deviceBrand", "null")
    overwriteFields.put("deviceType", "null")
    overwriteFields.put("browserKernel", "null")
    try {
      // 可选配置
      globalConfig.getConfigList(s"modules.$moduleName.dwr.acc.day.overwrite").foreach { x =>
        overwriteFields.put(x.getString("as"), x.getString("expr"))
      }
    } catch {
      case e: Exception =>
    }
    val fields = dwrFields.map { x =>
      if (x.equals("l_time")) {
        "date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time"
      } else if (x.equals("b_date")) {
        "date_format(b_date, 'yyyy-MM-dd') as b_date"
      } else if (x.equals("b_time")) {
        "date_format(b_time, 'yyyy-MM-dd 00:00:00') as b_time"
      } else if (overwriteFields.contains(x)) {
        s"""${overwriteFields.get(x)} as $x"""
      } else {
        x
      }
    }

    cookie = hiveClient.overwriteUnionSum(
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      table,
      persistenceDwr.selectExpr(fields:_*),
      aggExprsAlias,
      unionAggExprsAndAlias,
      overwriteAggFields,
      groupByExprsAlias,
      partitionFields.head,
      partitionFields.tail:_*
    )

    batchTransactionCookiesCache.add(cookie)
    persistenceDwr
  }

  override def commit(c: TransactionCookie): Unit = {
    hiveClient.commit(cookie)
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    var result = Array[TransactionCookie]()

    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
    if (mixTransactionManager.needTransactionalAction()) {
      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
      result = needCleans.toArray
      batchTransactionCookiesCache.removeAll(needCleans)
    }
    hiveClient.clean(result:_*)
  }
}
