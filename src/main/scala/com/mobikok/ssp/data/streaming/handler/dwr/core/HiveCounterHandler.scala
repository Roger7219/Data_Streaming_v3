package com.mobikok.ssp.data.streaming.handler.dwr.core

import java.util
import java.util.List

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.{HiveTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConversions._

class HiveCounterHandler extends Handler with Persistence {

  val LOG: Logger = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  var counterAliasMaps: Map[String, String] = _
  // ${a,b}.count>10 转化为：
  // gsExpr    : "a,b"
  // tableAlias: "${a,b}.count>10"
  // where     : "`${a,b}.count>10`.count>10"
  var counterWhereExprs: Set[(String, String, String)] = _ //Set[Array[String]] = _

  var aggExprs: List[Column] = _
  @volatile var batchTransactionCookiesCache
  : util.List[TransactionCookie] = new util.ArrayList[TransactionCookie]()
  @volatile var currentBatchCookies: util.List[TransactionCookie] = _

  var dwrTable: String = _
  var aggExprsAlias: List[String] = _
  var unionAggExprsAndAlias: List[Column] = _
  var overwriteAggFields: Set[String] = _
  var groupByExprsAlias: Array[String] = _
  var partitionFields: Array[String] = _

  var isCacheRefreshing  = false

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, exprString: String, as: String): Unit = {
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, exprString, as)
    isAsynchronous = true
    dwrTable = globalConfig.getString(s"modules.$moduleName.dwr.table")

    aggExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList

    unionAggExprsAndAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("union")).as(x.getString("as"))
    }.toList

    overwriteAggFields = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => if(x.hasPath("overwrite") && x.getBoolean("overwrite")) x.getString("as") else null
    }.filter(_ != null).toSet

    groupByExprsAlias = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => x.getString("as")
    }.toArray

    counterAliasMaps = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map {
      x => if(x.hasPath("map")) x.getString("as") -> x.getString("map") else null
    }.filter(_ != null).toMap

    partitionFields = Array("l_time", "b_date", "b_time", "b_version")

    aggExprs = globalConfig.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => expr(x.getString("expr")).as(x.getString("as"))
    }.toList

    // <"a,b", "${a,b}.count>10", "`${a,b}.count>10`.count>10">
    counterWhereExprs = counterAliasMaps
      // [${a,b}.count>10, ${a,b}.count>20]
      .map{case(_, map)=> StringUtil.getMatcher("(\\{.+?\\}.+?)\\s*(?:,|[oO][rR]|[aA][nN][dD])", StringUtil.standardizedSQL(map))}
      .flatMap{x=>x}
      .toSet[String]
      .map{x=>(
        // gsExpr    : "a,b"
        // tableAlias: "${a,b}.count>10"
        // where     : "`${a,b}.count>10`.count>10"
        StringUtil.getFirstMatcher("\\{(.+?)\\}\\.", x),
        x,
        x.replaceAll("\\{(.+?)}", "`" + x + "`")
      )}

    System.out.println(
        "counterWhereExprs1: " + counterWhereExprs + "\n"
    )

    // 确保创建了对应的表
    counterWhereExprs.groupBy(_._1).foreach{ case(gsExpr, iter)=>
      val t = HiveCounterHandler.countTableName(dwrTable, gsExpr.split(","))
      sql(s"create table if not exists $t like $dwrTable")
    }

    refreshCache(CSTTime.now.modifyHourAsBTime(0))
  }

  override def handle(persistenceDwr: DataFrame): DataFrame = {
    currentBatchCookies = new util.ArrayList[TransactionCookie]()

    if(counterWhereExprs.nonEmpty){

      val fs = persistenceDwr.schema.fields

      counterWhereExprs.groupBy(_._1).par.foreach { case(gsExpr, _) =>

        val gs = gsExpr.split(",")
        val t = HiveCounterHandler.countTableName(dwrTable, gs)

        val cs = fs.map { f =>
          // 是维度字段 且 无需单维度统计的字段 置为null
          if(groupByExprsAlias.contains(f.name) && !gs.contains(f.name))
            expr("null").cast(f.dataType).as(f.name)
          else
            col(f.name)
        }

        val resultDF = persistenceDwr
          .groupBy((gs ++ partitionFields).map(col(_)):_*)
          .agg(unionAggExprsAndAlias.head, unionAggExprsAndAlias.tail: _*)
          .select(cs: _*)

        currentBatchCookies.add(
          hiveClient.overwriteUnionSum(
            transactionManager.asInstanceOf[MixTransactionManager].getCurrentTransactionParentId(),
            t,
            resultDF,
            aggExprsAlias,
            unionAggExprsAndAlias,
            overwriteAggFields,
            groupByExprsAlias,
            null,
            partitionFields.head,
            partitionFields.tail:_*
          )
        )

        batchTransactionCookiesCache.addAll(currentBatchCookies)

      }

    }

    persistenceDwr
  }

  def refreshCache(b_times: String*): Unit ={

    if(!isCacheRefreshing && b_times.nonEmpty) {

      isCacheRefreshing = true

      // 异步执行
      new Thread(new Runnable {
        override def run(): Unit = {

          RunAgainIfError.run{

            counterWhereExprs.groupBy(_._2).par.foreach{ case(_, iter)=>

              iter.foreach{case(gsExpr, whereTableAlias, where)=>

                val gs = gsExpr.split(",")
                val t = HiveCounterHandler.countTableName(dwrTable, gsExpr.split(","))
                LOG.warn("Refresh counter cache start", "countTable", t, "whereTableAlias", whereTableAlias, "where", where, "b_time", b_times)

                // 随机数用于避免读取缓存，读取表最新数据(待验证)
                // 广播并缓存，用于mapjoin
                var c = hiveContext
                  .read
                  .table(t)
                  .where(s"b_time in ${b_times.mkString("('", "','", "')")}")
                  // 需要重新group by，将l_time=0001-01-01 00:00:00分区和普通l_time分区数据合并
                  .groupBy((gs :+ "b_time").map(col(_)):_*)
                  .agg(unionAggExprsAndAlias.head, unionAggExprsAndAlias.tail: _*)
                  .alias(whereTableAlias)
                  .where(where)

                c = broadcast(hiveContext.createDataFrame(c.collectAsList(), c.schema)).cache()

                // 释放上次的缓存
                val lastC: DataFrame = HiveCounterHandler.CACHE_MAP.get(whereTableAlias)
                if(lastC != null) HiveCounterHandler.NEED_UNPERSIST_COUNTERS.add(lastC)

                HiveCounterHandler.CACHE_MAP.put(whereTableAlias, c)

                LOG.warn("Refresh counter cache done", "countTable", t, "whereTableAlias", whereTableAlias, "where", where, "b_time", b_times)

              }

            }
          }

          isCacheRefreshing = false
        }
      }).start();

    }

  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def commit(c: TransactionCookie): Unit = {
    var bts = new util.HashSet[String]()
    currentBatchCookies.par.foreach{ cookie=>

      hiveClient.commit(cookie)

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

      bts.addAll(dwrT
        .partitions
        .flatMap{x=>x}
        .filter{x=>"b_time".equals(x.name)}
        .map{x=>x.value}
        .toSet[String]
      )
    }

    currentBatchCookies = null

    refreshCache(bts.toArray(new Array[String](0)):_*)

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

object HiveCounterHandler{
  private var NEED_UNPERSIST_COUNTERS = new util.ArrayList[DataFrame]()
  private var CACHE_MAP: util.Map[String, DataFrame] = new util.HashMap()

  private def countTableName(dwrTable: String, fields: Array[String]): String = {
    // 取列名前3位，待优化
    s"${dwrTable}__${fields.map{x=> if(x.length > 3) x.substring(0, 3) else x}.mkString("_")}__counter"
//    s"${dwrTable}_counter"
  }

  def borrowCounter(dwrTable: String, whereTableAlias: String, buildEmptyDFCallback: => DataFrame): DataFrame = {
//    val t = countTableName(dwrTable, where)
    var df = CACHE_MAP.get(whereTableAlias)
    if(df == null) df = buildEmptyDFCallback
    df.as(whereTableAlias)
  }

  def revertCounters(borrowedCounters: util.ArrayList[DataFrame]): Unit ={
    for(c <- borrowedCounters){
      if(NEED_UNPERSIST_COUNTERS.contains(c)){
        // 释放counter缓存
        c.unpersist()
      }
    }

    // 清理记录
    for(c <- borrowedCounters){
      if(NEED_UNPERSIST_COUNTERS.contains(c)){
        NEED_UNPERSIST_COUNTERS.remove(c)
      }
    }


  }


}