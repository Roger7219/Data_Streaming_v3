package com.mobikok.ssp.data.streaming.handler.dwr.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConversions._

class HiveDWRPersistHandler extends Handler with Persistence {

//  val COOKIE_KIND_DWR_T = "dwrT"

  var table: String = _
  var cookie: TransactionCookie = _

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, expr: String, as: String): Unit = {
    // 默认为true 遵从配置设置
    isAsynchronous = true
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, expr, as)
    table = handlerConfig.getString("table")
    cookieKindMark = handlerConfig.getString("cookie.kind.mark")
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def handle(persistenceDwr: DataFrame): (String, DataFrame, TransactionCookie) = {
    val aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList

    val unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    val groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

    val partitionFields = globalConfig.getStringList(s"modules.$moduleName.dwr.partition.fields")

    cookie = hiveClient.overwriteUnionSum(
      transactionParentId,
      table,
      persistenceDwr,
      aggExprsAlias,
      unionAggExprsAndAlias,
      groupByExprsAlias,
      partitionFields.head,
      partitionFields.tail:_*
    )
    (cookieKindMark, persistenceDwr, cookie)
  }

  override def commit(cookie: TransactionCookie): Unit = {
    hiveClient.commit(this.cookie)
  }


  override def clean(cookies: TransactionCookie*): Unit = {
    hiveClient.clean(this.cookie)
    this.cookie = null
  }
}
