package com.mobikok.ssp.data.streaming.handler.dwi

import java.util

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

class NadxPerformanceHandler extends Handler {

  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

  var unmatchedPerformanceDwiTable = "nadx_overall_performance_unmatched_dwi"
  var trafficTable = "nadx_overall_traffic_dwi"
  var cookie: TransactionCookie = _
  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  var joinedDF: DataFrame = null
  var unMatchedPerformanceDf: DataFrame = null

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, handlerConfig, globalConfig, expr, as)
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"NadxPerformanceHandler handle start")

    val unM = selectUnMatchedPerformanceByTrafficDWIPartitionMessage()
    val newDWIBTs = collectNewDWIBTimes(newDwi)

    val df = joinTrafficData(newDwi, newDWIBTs, unM._1,  unM._2)

    cookie = saveUnMatchedPerformance(df._2)

    batchTransactionCookiesCache.add(cookie)

    LOG.warn(s"NadxPerformanceHandler handle done")
    (df._1, Array())
  }

  def selectUnMatchedPerformanceByTrafficDWIPartitionMessage(): (DataFrame, Array[Array[HivePartitionPart]]) = {

    var ps: Array[Array[HivePartitionPart]] = Array()

    // 读取hive以前没有匹配到的Performance数据
    // pullBTimeDescTail无值时不调用，pullBTimeDesc无值时也调用，待改
    MC.pullBTimeDesc("NadxPerformanceHandlerCer", Array(trafficTable), {x=>

      if(x.size > 0) {
        var bts = x.map{y=>Array(y)}.toArray[Array[HivePartitionPart]]
        val where = hiveClient.partitionsWhereSQL(bts)

        // b_version +1是为了再次写入（如果没有匹配到流量数据）
        RunAgainIfError.run{
          unMatchedPerformanceDf = sql(
            s"""
               |select
               |  repeats,
               |  rowkey,
               |
               |  `type`,
               |  bidTime,
               |  supplyid,
               |  bidid,
               |  impid,
               |  price,
               |  cur,
               |  withPrice,
               |  eventType,
               |
               |  repeated,
               |  l_time,
               |  b_date,
               |  b_time,
               |  cast(cast(b_version as int) + 1 as string) as b_version
               |
               |from $unmatchedPerformanceDwiTable where ( $where ) AND (b_time, b_version) in (
               |  select b_time, b_version
               |  from (
               |    select
               |      row_number() over(partition by b_time order by cast(b_version as int) desc) as version_num,
               |      b_time,
               |      b_version
               |    from $unmatchedPerformanceDwiTable
               |    where $where
               |  )t0 where version_num = 1
               |)
               |""".stripMargin)

          unMatchedPerformanceDf.cache()

          val ts = Array("repeated", "l_time", "b_date", "b_time", "b_version")

          ps = unMatchedPerformanceDf
            .dropDuplicates(ts)
            .collect()
            .map { x =>
              ts.map { y =>
                HivePartitionPart(y, x.getAs[String](y))
              }
            }
        }
      }

      true
    })

    (unMatchedPerformanceDf, ps)
  }


  def collectNewDWIBTimes(newDwi: DataFrame): Array[Array[HivePartitionPart]] = {
    val ts = Array("l_time", "b_date", "b_time")
    var result = newDwi
      .dropDuplicates(ts)
      .collect()
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }

    LOG.warn("collectNewDWIBTimes",  result)
    result
  }

  def selectTrafficByNewDWIBTimes(bts: Set[String]): DataFrame = {
    sql(s"select * from $trafficTable where b_time = ")
  }

  def joinTrafficData(newPerformanceDwi: DataFrame, newDwiBts: Array[Array[HivePartitionPart]], unMatchedPerformanceDwi: DataFrame, unMatchedDwiBts: Array[Array[HivePartitionPart]]): (DataFrame, DataFrame) = {

    // 合并
    var df: DataFrame = if(unMatchedPerformanceDwi != null) newPerformanceDwi.union(unMatchedPerformanceDwi) else newPerformanceDwi
    df.createOrReplaceTempView("performanceDF")

    var bts = newDwiBts ++ unMatchedDwiBts
    val whereBTimes = hiveClient.partitionsWhereSQL(bts)

    joinedDF = sql(
      s"""
         |select
         |  repeats                           ,
         |  rowkey                            ,
         |
         |  type                              ,
         |  bidTime                           ,
         |  supplyid                          ,
         |  bidid                             ,
         |  impid                             ,
         |  price                             ,
         |  cur                               ,
         |  withPrice                         ,
         |  eventType                         ,
         |
         |  dataType                          ,
         |  `timestamp`                       ,
         |  supply_bd_id                      ,
         |  supply_am_id                      ,
         |  supply_id                         ,
         |  supply_protocol                   ,
         |  request_flag                      ,
         |
         |  ad_format                         ,
         |  site_app_id                       ,
         |  placement_id                      ,
         |  position                          ,
         |
         |  country                           ,
         |  state                             ,
         |  city                              ,
         |
         |  carrier                           ,
         |
         |  os                                ,
         |  os_version                        ,
         |
         |  device_type                       ,
         |  device_brand                      ,
         |  device_model                      ,
         |
         |  age                               ,
         |  gender                            ,
         |
         |  cost_currency                     ,
         |
         |  demand_bd_id                      ,
         |  demand_am_id                      ,
         |  demand_id                         ,
         |
         |  demand_seat_id                    ,
         |  demand_campaign_id                ,
         |  demand_protocol                   ,
         |  target_site_app_id                ,
         |  revenue_currency                  ,
         |
         |  bid_price_model                   ,
         |  traffic_type                      ,
         |  currency                          ,
         |
         |  supplyBidId                       ,
         |  bidRequestId                      ,
         |
         |  bundle                            ,
         |  size                              ,
         |
         |  supply_request_count              ,
         |  supply_invalid_request_count      ,
         |  supply_bid_count                  ,
         |  supply_bid_price_cost_currency    ,
         |  supply_bid_price                  ,
         |  supply_win_count                  ,
         |  supply_win_price_cost_currency    ,
         |  supply_win_price                  ,
         |
         |  demand_request_count              ,
         |  demand_bid_count                  ,
         |  demand_bid_price_revenue_currency ,
         |  demand_bid_price                  ,
         |  demand_win_count                  ,
         |  demand_win_price_revenue_currency ,
         |  demand_win_price                  ,
         |  demand_timeout_count              ,
         |
         |  impression_count                  ,
         |  impression_cost_currency          ,
         |  impression_cost                   ,
         |  impression_revenue_currency       ,
         |  impression_revenue                ,
         |  click_count                       ,
         |  click_cost_currency               ,
         |  click_cost                        ,
         |  click_revenue_currency            ,
         |  click_revenue                     ,
         |  conversion_count                  ,
         |  conversion_price                  ,
         |  saveCount                         ,
         |
         |  repeated                          ,
         |  l_time                            ,
         |  b_date                            ,
         |  b_time                            ,
         |  b_version
         |
         |from(
         |  select
         |    *,
         |    row_number() over(partition by dataType, bidRequestId order by 1 desc) row_num
         |  from (
         |
         |    select
         |      pDwi.repeats,
         |      pDwi.rowkey,
         |
         |      pDwi.type      AS type,
         |      pDwi.bidTime   AS bidTime,
         |      pDwi.supplyid  AS supplyid,
         |      pDwi.bidid     AS bidid,
         |      pDwi.impid     AS impid,
         |      pDwi.price     AS price,
         |      pDwi.cur       AS cur,
         |      pDwi.withPrice AS withPrice,
         |      pDwi.eventType AS eventType,
         |
         |      CASE pDwi.type
         |        WHEN 'win' then 5
         |        WHEN 'impression' then 6
         |        WHEN 'click' then 7
         |        WHEN 'conversion' then 8
         |        WHEN 'event' THEN (CASE pDwi.eventType
         |          WHEN 1 THEN 6
         |          WHEN 2 THEN 7
         |          WHEN 3 THEN 7
         |          WHEN 4 THEN 7
         |          ELSE null END
         |        )
         |        ELSE null END
         |      AS dataType,
         |
         |      tDwi.timestamp,
         |      tDwi.supply_bd_id,
         |      tDwi.supply_am_id,
         |      tDwi.supply_id,
         |      tDwi.supply_protocol,
         |      tDwi.request_flag,
         |
         |      tDwi.ad_format,
         |      tDwi.site_app_id,
         |      tDwi.placement_id,
         |      tDwi.position,
         |
         |      tDwi.country,
         |      tDwi.state,
         |      tDwi.city,
         |
         |      tDwi.carrier,
         |
         |      tDwi.os,
         |      tDwi.os_version,
         |
         |      tDwi.device_type,
         |      tDwi.device_brand,
         |      tDwi.device_model,
         |
         |      tDwi.age,
         |      tDwi.gender,
         |
         |      tDwi.cost_currency,
         |
         |      tDwi.demand_bd_id,
         |      tDwi.demand_am_id,
         |      tDwi.demand_id,
         |
         |      tDwi.demand_seat_id,
         |      tDwi.demand_campaign_id,
         |      tDwi.demand_protocol,
         |      tDwi.target_site_app_id,
         |      tDwi.revenue_currency,
         |
         |      tDwi.bid_price_model,
         |      tDwi.traffic_type,
         |      tDwi.currency,
         |      tDwi.supplyBidId,
         |      tDwi.bidRequestId,
         |
         |      tDwi.bundle,
         |      tDwi.size,
         |
         |      0  AS supply_request_count,
         |      0  AS supply_invalid_request_count,
         |      0  AS supply_bid_count,
         |      0. AS supply_bid_price_cost_currency,
         |      0. AS supply_bid_price,
         |
         |      CASE pDwi.type
         |        WHEN 'win' THEN 1
         |        ELSE 0 END
         |      AS supply_win_count,
         |
         |      CASE pDwi.type
         |        WHEN 'win' THEN (
         |         CASE
         |           WHEN (!pDwi.withPrice OR (pDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price_cost_currency
         |           ELSE (CASE
         |             WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
         |             ELSE winDwi.price*1.00000000 END
         |           ) END
         |        )
         |        ELSE 0. END
         |      AS supply_win_price_cost_currency,
         |
         |      CASE pDwi.type
         |        WHEN 'win' THEN (
         |         CASE
         |           WHEN (!winDwi.withPrice OR (winDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price
         |           ELSE (CASE
         |             WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
         |               WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
         |               ELSE winDwi.price*1.000000 END
         |             )
         |             ELSE (CASE
         |               WHEN pDwi.cur = tDwi.currency THEN winDwi.price
         |               ELSE winDwi.price*1.000000 END
         |             ) END
         |           ) END
         |        )
         |        ELSE 0. END
         |      AS supply_win_price,
         |
         |      0  as demand_request_count,
         |      0  as demand_bid_count,
         |      0. as demand_bid_price_revenue_currency,
         |      0. as demand_bid_price,
         |      0  as demand_win_count,
         |      0. as demand_win_price_revenue_currency,
         |      0. as demand_win_price,
         |      0  as demand_timeout_count,
         |
         |      CASE pDwi.type
         |        WHEN 'impression' THEN 1
         |        WHEN 'event' THEN (CASE pDwi.eventType
         |          WHEN 1 THEN 1
         |          ELSE 0 END
         |        )
         |        ELSE 0 END
         |      AS impression_count,
         |
         |      CASE pDwi.type
         |        WHEN 'impression' THEN (CASE
         |          WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price_cost_currency
         |          ELSE (CASE
         |            WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
         |            ELSE winDwi.price*1.0000000 END
         |          )END
         |        )
         |        ELSE 0. END
         |      AS impression_cost_currency,
         |
         |      CASE pDwi.type
         |        WHEN 'impression' THEN (CASE
         |          WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price
         |          ELSE (CASE
         |            WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
         |              WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
         |              ELSE winDwi.price*1.000000 END
         |            )
         |            ELSE (CASE
         |              WHEN pDwi.cur = tDwi.currency THEN winDwi.price
         |              ELSE winDwi.price*1.0000000 END
         |            ) END
         |          )END
         |        )
         |        ELSE 0. END
         |      AS impression_cost,
         |
         |      CASE pDwi.type
         |        WHEN 'impression' THEN tDwi.demand_win_price_revenue_currency
         |        ELSE 0. END
         |      AS impression_revenue_currency,
         |
         |      CASE pDwi.type
         |        WHEN 'impression' THEN tDwi.demand_win_price
         |        ELSE 0. END
         |      AS impression_revenue,
         |
         |      CASE pDwi.type
         |        WHEN 'click' THEN 1
         |        WHEN 'event' THEN (CASE pDwi.eventType
         |          WHEN 2 THEN 1
         |          WHEN 3 THEN 1
         |          WHEN 4 THEN 1
         |          ELSE 0 END
         |        )
         |        ELSE 0 END
         |      AS click_count,
         |
         |      0. as click_cost_currency,
         |      0. as click_cost,
         |      0. as click_revenue_currency,
         |      0. as click_revenue,
         |
         |      CASE pDwi.type
         |        WHEN 'conversion' THEN 1
         |        ELSE 0 END
         |      AS conversion_count,
         |
         |      0. as conversion_price,
         |
         |      0 as saveCount,
         |
         |      pDwi.repeated,
         |      '${transactionManager.asInstanceOf[MixTransactionManager].dwiLoadTime()}' as l_time,
         |      pDwi.b_date,
         |      pDwi.b_time,
         |      pDwi.b_version as b_version
         |    from (select * from performanceDF where ( ${whereBTimes} ) ) pDwi
         |    left join (
         |      select * from $trafficTable where ( ${whereBTimes} ) and dataType = 4
         |    ) tDwi ON tDwi.b_time = pDwi.b_time AND pDwi.bidid = tDwi.bidRequestId
         |    left join (
         |     select * from performanceDF where ( ${whereBTimes} ) AND repeated = 'N' AND type = 'win'
         |    ) winDwi ON winDwi.b_time = pDwi.b_time AND winDwi.bidid = pDwi.bidid
         |
         |  )t0
         |)t1
         |where row_num = 1
       """.stripMargin)

      joinedDF.persist(StorageLevel.MEMORY_ONLY_SER)

      var result = (
        joinedDF.where(expr("bidRequestId is not null")),
        joinedDF.where(expr("bidRequestId is null")).selectExpr(
          "repeats",
          "rowkey",

          "`type`",
          "bidTime",
          "supplyid",
          "bidid",
          "impid",
          "price",
          "cur",
          "withPrice",
          "eventType",

          "repeated",
          "l_time",
          "b_date",
          "b_time",
          "b_version"
        )
      )
      result
  }

  def saveUnMatchedPerformance(df: DataFrame/*, updateVersionBTimes: Array[Array[HivePartitionPart]]*/) = {
    val tm = transactionManager.asInstanceOf[MixTransactionManager]

    val pTid = tm.getCurrentTransactionParentId()

    val partitionFields = Array("repeated", "l_time", "b_date", "b_time", "b_version")
    val ps = df
      .dropDuplicates(partitionFields)
      .collect()
      .map { x =>
        partitionFields.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }

    // 必须创建分区，因为b_version是根据分区来识别的，空分区说明该版本数据为空
    hiveClient.partitionsAlterSQL(ps).foreach{x=>
      sql(s"alter table ${unmatchedPerformanceDwiTable} add if not exists partition($x)")
    }


    hiveClient.into(pTid, unmatchedPerformanceDwiTable, df, ps)
  }

  override def init (): Unit = {}

  override def commit (cookies: TransactionCookie): Unit = {
    hiveClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    joinedDF.unpersist()
    if(unMatchedPerformanceDf != null) unMatchedPerformanceDf.unpersist()

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
