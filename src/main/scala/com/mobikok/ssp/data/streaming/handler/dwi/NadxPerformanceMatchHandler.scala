package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class NadxPerformanceMatchHandler extends Handler {

//  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()

//  var unmatchedPerformanceDwiTable = "nadx_overall_performance_unmatched_dwi"
//  var trafficTable = "nadx_overall_traffic_dwi"
//  var cookie: TransactionCookie = _
//  var dwiBTimeFormat = "yyyy-MM-dd HH:00:00"
  var joinedDF: DataFrame = null
//  var unMatchedPerformanceDf: DataFrame = null

  var unmatchedKafkaSender: KafkaSender = KafkaSender.instance("performance_unmatched_topic")
  var matchedKafkaSender: KafkaSender = KafkaSender.instance("performance_matched_topic")

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"NadxPerformanceMatchHandler handle start")

    val bts = collectDWIBTimes(newDwi)
    val whereBTimes = hiveClient.partitionsWhereSQL(bts)

    joinedDF = sql(
      s"""
         |select
         |  pDwi.repeats,
         |  pDwi.rowkey,
         |
         |  pDwi.type      AS type,
         |  pDwi.bidTime   AS bidTime,
         |  pDwi.supplyid  AS supplyid,
         |  pDwi.bidid     AS bidid,
         |  pDwi.impid     AS impid,
         |  pDwi.price     AS price,
         |  pDwi.cur       AS cur,
         |  pDwi.withPrice AS withPrice,
         |  pDwi.eventType AS eventType,
         |
         |  CASE pDwi.type
         |    WHEN 'win' then 5
         |    WHEN 'impression' then 6
         |    WHEN 'click' then 7
         |    WHEN 'conversion' then 8
         |    WHEN 'event' THEN (CASE pDwi.eventType
         |      WHEN 1 THEN 6
         |      WHEN 2 THEN 7
         |      WHEN 3 THEN 7
         |      WHEN 4 THEN 7
         |      ELSE null END
         |    )
         |    ELSE null END
         |  AS dataType,
         |
         |  tDwi.timestamp,
         |  tDwi.supply_bd_id,
         |  tDwi.supply_am_id,
         |  tDwi.supply_id,
         |  tDwi.supply_protocol,
         |  tDwi.request_flag,
         |
         |  tDwi.ad_format,
         |  tDwi.site_app_id,
         |  tDwi.placement_id,
         |  tDwi.position,
         |
         |  tDwi.country,
         |  tDwi.state,
         |  tDwi.city,
         |
         |  tDwi.carrier,
         |
         |  tDwi.os,
         |  tDwi.os_version,
         |
         |  tDwi.device_type,
         |  tDwi.device_brand,
         |  tDwi.device_model,
         |
         |  tDwi.age,
         |  tDwi.gender,
         |
         |  tDwi.cost_currency,
         |
         |  tDwi.demand_bd_id,
         |  tDwi.demand_am_id,
         |  tDwi.demand_id,
         |
         |  tDwi.demand_seat_id,
         |  tDwi.demand_campaign_id,
         |  tDwi.demand_protocol,
         |  tDwi.target_site_app_id,
         |  tDwi.revenue_currency,
         |
         |  tDwi.bid_price_model,
         |  tDwi.traffic_type,
         |  tDwi.currency,
         |  tDwi.supplyBidId,
         |  tDwi.bidRequestId,
         |--   pDwi.bidid AS performanceBidRequestId,
         |
         |  tDwi.bundle,
         |  tDwi.size,
         |
         |  0  AS supply_request_count,
         |--   待删
         |  0  AS supply_invalid_request_count,
         |  0  AS supply_bid_count,
         |  0. AS supply_bid_price_cost_currency,
         |  0. AS supply_bid_price,
         |
         |  CASE pDwi.type
         |    WHEN 'win' THEN 1
         |    ELSE 0 END
         |  AS supply_win_count,
         |
         |  CASE pDwi.type
         |    WHEN 'win' THEN (
         |     CASE
         |       WHEN (!pDwi.withPrice OR (pDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price_cost_currency
         |       ELSE (CASE
         |         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
         |         ELSE winDwi.price*1.00000000 END
         |       ) END
         |    )
         |    ELSE 0. END
         |  AS supply_win_price_cost_currency,
         |
         |  CASE pDwi.type
         |    WHEN 'win' THEN (
         |     CASE
         |       WHEN (!winDwi.withPrice OR (winDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price
         |       ELSE (CASE
         |         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
         |           WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
         |           ELSE winDwi.price*1.000000 END
         |         )
         |         ELSE (CASE
         |           WHEN pDwi.cur = tDwi.currency THEN winDwi.price
         |           ELSE winDwi.price*1.000000 END
         |         ) END
         |       ) END
         |    )
         |    ELSE 0. END
         |  AS supply_win_price,
         |
         |  0  as demand_request_count,
         |  0  as demand_bid_count,
         |  0. as demand_bid_price_revenue_currency,
         |  0. as demand_bid_price,
         |  0  as demand_win_count,
         |  0. as demand_win_price_revenue_currency,
         |  0. as demand_win_price,
         |  0  as demand_timeout_count,
         |
         |  CASE pDwi.type
         |    WHEN 'impression' THEN 1
         |    WHEN 'event' THEN (CASE pDwi.eventType
         |      WHEN 1 THEN 1
         |      ELSE 0 END
         |    )
         |    ELSE 0 END
         |  AS impression_count,
         |
         |  CASE pDwi.type
         |    WHEN 'impression' THEN (CASE
         |      WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price_cost_currency
         |      ELSE (CASE
         |        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
         |        ELSE winDwi.price*1.0000000 END
         |      )END
         |    )
         |    ELSE 0. END
         |  AS impression_cost_currency,
         |
         |  CASE pDwi.type
         |    WHEN 'impression' THEN (CASE
         |      WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price
         |      ELSE (CASE
         |        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
         |          WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
         |          ELSE winDwi.price*1.000000 END
         |        )
         |        ELSE (CASE
         |          WHEN pDwi.cur = tDwi.currency THEN winDwi.price
         |          ELSE winDwi.price*1.0000000 END
         |        ) END
         |      )END
         |    )
         |    ELSE 0. END
         |  AS impression_cost,
         |
         |  CASE pDwi.type
         |    WHEN 'impression' THEN tDwi.demand_win_price_revenue_currency
         |    ELSE 0. END
         |  AS impression_revenue_currency,
         |
         |  CASE pDwi.type
         |    WHEN 'impression' THEN tDwi.demand_win_price
         |    ELSE 0. END
         |  AS impression_revenue,
         |
         |  CASE pDwi.type
         |    WHEN 'click' THEN 1
         |    WHEN 'event' THEN (CASE pDwi.eventType
         |      WHEN 2 THEN 1
         |      WHEN 3 THEN 1
         |      WHEN 4 THEN 1
         |      ELSE 0 END
         |    )
         |    ELSE 0 END
         |  AS click_count,
         |
         |  0. as click_cost_currency,
         |  0. as click_cost,
         |  0. as click_revenue_currency,
         |  0. as click_revenue,
         |
         |  CASE pDwi.type
         |    WHEN 'conversion' THEN 1
         |    ELSE 0 END
         |  AS conversion_count,
         |
         |  0. as conversion_price,
         |
         |  0 as saveCount,
         |
         |  tDwi.bidfloor as bidfloor,
         |  tDwi.site_id as site_id,
         |  tDwi.site_cat as site_cat,
         |  tDwi.site_domain as site_domain,
         |  tDwi.publisher_id as publisher_id,
         |  tDwi.app_id as app_id,
         |  tDwi.tmax as tmax,
         |  tDwi.ip as ip,
         |  tDwi.crid as crid,
         |  tDwi.cid as cid,
         |--   待删
         |  cast(null as string) as tips,
         |  pDwi.node as node,
         |  pDwi.tip_type,
         |  pDwi.tip_desc,
         |  tDwi.adm,
         |
         |  CASE pDwi.type
         |    WHEN 'event' THEN 1
         |    ELSE 0 END
         |  AS event_count,
         |  tDwi.ssp_token,
         |
         |  pDwi.repeated,
         |  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:00:00') as l_time,
         |  pDwi.b_date,
         |  pDwi.b_time,
         |  pDwi.b_version as b_version
         |
         |from (
         |  select * from nadx_performance_dwi where repeated = 'N' AND b_time
         |) pDwi
         |left join (
         |  select * from nadx_traffic_dwi where dataType = 4 AND ${whereBTimes}
         |) tDwi ON tDwi.b_time = pDwi.b_time AND pDwi.bidid = tDwi.bidRequestId
         |left join (
         |  select * from nadx_performance_dwi where repeated = 'N' AND ${whereBTimes} AND type = 'win'
         |) winDwi ON winDwi.b_time = pDwi.b_time AND winDwi.bidid = pDwi.bidid
         |
         |
       """.stripMargin)
      .as("nadx_performance_joined_dwi_tmp")
      .cache()


    var matchedDF = sql(
      s"""
         |select
         |  repeats                          ,
         |  rowkey                            ,
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
         |-- demand
         |  demand_bd_id                      ,
         |  demand_am_id                      ,
         |  demand_id                         ,
         |
         |-- destination
         |  demand_seat_id                    ,
         |  demand_campaign_id                ,
         |  demand_protocol                   ,
         |  target_site_app_id                ,
         |  revenue_currency                  ,
         |
         |-- common
         |  bid_price_model                   ,
         |  traffic_type                      ,
         |  currency                          ,
         |
         |-- id
         |  supplyBidId                       ,
         |  bidRequestId                      ,
         |
         |  bundle                            ,
         |  size                              ,
         |
         |  supply_request_count              ,
         |--   待删，冗余
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
         |  bidfloor                          ,
         |  site_id                           ,
         |  site_cat                          ,
         |  site_domain                       ,
         |  publisher_id                      ,
         |  app_id                            ,
         |  tmax                              ,
         |  ip                                ,
         |  crid                              ,
         |  cid                               ,
         |  tips                              ,
         |  node                              ,
         |  tip_type                          ,
         |  tip_desc                          ,
         |  adm                               ,
         |
         |  event_count                       ,
         |  ssp_token                         ,
         |
         |  repeated                          ,
         |  l_time                            ,
         |  b_date                            ,
         |  b_time                            ,
         |  b_version
         |from nadx_performance_joined_dwi_tmp
         |where bidRequestId is not null
       """.stripMargin)

    var unmatchedDF = sql(
      s"""
         |select
         |  type,
         |  bidTime,
         |  supplyid,
         |  bidid,
         |  impid,
         |  price,
         |  cur,
         |  withPrice,
         |  eventType,
         |  node,
         |  tip_type,
         |  tip_desc
         |from nadx_performance_joined_dwi_tmp
         |where bidRequestId is null
       """.stripMargin)

    matchedKafkaSender.send(matchedDF.toJSON.collectAsList())
    unmatchedKafkaSender.send(unmatchedDF.toJSON.collectAsList())

//    batchTransactionCookiesCache.add(cookie)

    LOG.warn(s"NadxPerformanceMatchHandler handle done")
    (matchedDF, Array())
  }

  def collectDWIBTimes(newDwi: DataFrame): Array[Array[HivePartitionPart]] = {
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

  override def init (): Unit = {}

  override def commit (cookies: TransactionCookie): Unit = {
//    hiveClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    if(joinedDF != null) joinedDF.unpersist()

//    var result = Array[TransactionCookie]()
//
//    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
//    if (mixTransactionManager.needTransactionalAction()) {
//      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
//      result = needCleans.toArray
//      batchTransactionCookiesCache.removeAll(needCleans)
//    }
//    hiveClient.clean(result:_*)
  }
}
