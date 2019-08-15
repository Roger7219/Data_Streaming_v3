package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util
import java.util.List

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.expr

import scala.collection.JavaConversions._

@deprecated
class HiveDWRPersistHandler extends Handler with Persistence {

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())
//  val COOKIE_KIND_DWR_T = "dwrT"

  var table: String = _
  var cookie: TransactionCookie = _
  var needCounterMapFields:Set[String] = _

  var aggExprsAlias: List[String] = _
  var unionAggExprsAndAlias: List[Column] = _
  var overwriteAggFields: Set[String] = _
  var groupByExprsAlias: Array[String] = _

  var counterAliasMaps: Map[String, String] = _
  // gsExpr         : "a,b"
  // whereTableAlias: "`{a,b}.count>10`"
  // where          : "`{a,b}.count>10`.count>10"
  var counterWhereExprs: Set[(String, String, String)] = _
  // <`{a,b}.count>10`, `{a,b}.count>10`.count>10">
  var counterAliasReplacedMap: Map[String, String] = _

//  var counterTablePrefix = "___"
  var partitionFields: Array[String] = _

  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, exprString: String, as: String): Unit = {
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, exprString, as)
    // 异步
    isAsynchronous = true
    table = globalConfig.getString(s"modules.$moduleName.dwr.table")

    aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
//
    unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    overwriteAggFields = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => if(x.hasPath("overwrite") && x.getBoolean("overwrite")) x.getString("as") else null
    }.filter(_ != null).toSet

    groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

    partitionFields = Array("l_time", "b_date", "b_time", "b_version")

    counterAliasMaps = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => if(x.hasPath("map")) x.getString("as") -> x.getString("map") else null
    }.filter(_ != null).toMap

    // gsExpr    : "a,b"
    // tableAlias: "`{a,b}.count>10`"
    // where     : "`{a,b}.count>10`.count>10"
    counterWhereExprs = counterAliasMaps
      // [${a,b}.count>10, ${a,b}.count>20]
      .map{case(_, map)=> StringUtil.getMatcher("(\\{.+?\\}.+?)\\s*(?:,|[oO][rR]|[aA][nN][dD])", StringUtil.standardizedSQL(map))}
      .flatMap{x=>x}
      .toSet[String]
      .map{x=>(
        StringUtil.getFirstMatcher("\\{(.+?)\\}\\.", x),
        x,
        x.replaceAll("\\{(.+?)}", "`" + x + "`")
      )}

    counterAliasReplacedMap = counterAliasMaps.map{case(as, map)=>
      as-> StringUtil.standardizedSQL(map).replaceAll("(\\{.+?}(.+?))(\\s*)(,|[oO][rR]|[aA][nN][dD])", "`$1`$2$3$4")
    }

    // 判断需要统计的维度字段
    needCounterMapFields = counterAliasMaps.map{ case(_, map)=>
      StringUtil.getMatcher("\\$(.+?)\\.", map)
    }.flatMap{x=>x}.toSet
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  var borrowedCounters: util.ArrayList[DataFrame] = _

  override def handle(persistenceDwr: DataFrame): DataFrame = {

    borrowedCounters = new util.ArrayList[DataFrame]()

    cookie = hiveClient.overwriteUnionSum(
      transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
      table,
      persistenceDwr,
      aggExprsAlias,
      unionAggExprsAndAlias,
      overwriteAggFields,
      groupByExprsAlias,
      { df =>
        if(counterWhereExprs.nonEmpty) {
          var joinedDF = df.as("dwr")

          counterWhereExprs.foreach{ case(gsExpr, whereTableAlias, _)=>

            val c = HiveCounterHandler.borrowCounter(table, whereTableAlias, hiveClient.emptyDF(persistenceDwr.schema))

            joinedDF = joinedDF.join(
              c,
              expr(gsExpr
                .split(",")
                .map{g=>s"dwr.$g = `$whereTableAlias`.$g"}
                .mkString("(", " and ", s") and dwr.b_time = `$whereTableAlias`.b_time ")),
              "left_outer"
            )

            borrowedCounters.add(c)
          }

          joinedDF.select(df.schema.fieldNames.map{x=>expr(counterAliasReplacedMap.getOrElse(x, s"dwr.`$x`")).as(x)}:_*)

        }else {
          df
        }
      },
      partitionFields.head,
      partitionFields.tail:_*
    )

    batchTransactionCookiesCache.add(cookie)
    persistenceDwr
  }

  override def prepare(dwi: DataFrame): DataFrame = {
    dwi
//    if(counterMaps.nonEmpty) {
//
//      var joinedDwi = dwi.as("dwi")
//      counterMaps.foreach{case (f, _)=>
//
//        joinedDwi = joinedDwi.join(
//          HiveCounterHandler.borrowCounter(table, f, hiveClient.emptyDF(_)).as(s"$counterTablePrefix$f"),
//          expr(s"dwi.$f = $counterTablePrefix$f.$f and dwi.b_time = $counterTablePrefix$f.b_time "),
//          "left_outer"
//        )
//      }
//
//      joinedDwi.select(dwi.schema.fieldNames.map{x=>
//        expr(counterMaps
//          .getOrElse(x, s"dwi.`$x`")
//          .replaceAll("\\$", counterTablePrefix)
//        ).as(x)
//      }:_*)
//
//    }else {
//      dwi
//    }
  }

  override def commit(c: TransactionCookie): Unit = {
    hiveClient.commit(cookie)

    HiveCounterHandler.revertCounters(borrowedCounters)

    // push message
    val dwrT = cookie.asInstanceOf[HiveTransactionCookie]
    val topic = dwrT.targetTable
    if (dwrT != null && dwrT.partitions != null && dwrT.partitions.nonEmpty) {
      val key = OM.toJOSN(dwrT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      ThreadPool.LOCK.synchronized {
        MC.push(PushReq(topic, key))
      }
      //      messageClient.pushMessage(new MessagePushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")
    } else {
      LOG.warn(s"MessageClient dwr no hive partitions to push", s"topic: $topic")
    }
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
