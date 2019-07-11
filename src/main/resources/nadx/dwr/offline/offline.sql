-----------------------------------------------------------------------------------------
-- 离线统计
-----------------------------------------------------------------------------------------
-- spark-sql --driver-memory  8G   --executor-memory 12G
-- spark-sql --master yarn --hivevar start_b_time="`date "+%Y-%m-%d 00:00:00" -d "-1 days"`" --hivevar end_b_time="`date "+%Y-%m-%d 23:00:00" -d "-1 days"`" --hivevar b_date="`date "+%Y-%m-%d" -d "-1 days"`" -f ~/offline.crontab.sql  > offline.log 2>&1

-- beeline -n "" -p ""  -u jdbc:hive2://master:10016/default --outputformat=table --verbose=true

-- 强烈建议用Orc格式，避免字段值含分隔符导致数据错位问题！
set hive.default.fileformat=Orc;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.default.parallelism = 4;
set spark.sql.shuffle.partitions = 4;
set start_b_time = "${start_b_time}";
set end_b_time   = "${end_b_time}";
-- set b_date = "${b_date}";

set start_b_time;
set end_b_time;
-- set b_date;

-- set start_b_time = "2019-03-26 00:00:00";
-- set end_b_time   = "2019-03-26 23:00:00";
-- set b_date = "2019-03-26";

set log = "MAKE nadx_performance_matched_dwi_tmp START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

drop table if exists nadx_performance_matched_dwi_tmp;
create table nadx_performance_matched_dwi_tmp as
select
  pDwi.repeats,
  pDwi.rowkey,

  pDwi.type      AS type,
  pDwi.bidTime   AS bidTime,
  pDwi.supplyid  AS supplyid,
  pDwi.bidid     AS bidid,
  pDwi.impid     AS impid,
  pDwi.price     AS price,
  pDwi.cur       AS cur,
  pDwi.withPrice AS withPrice,
  pDwi.eventType AS eventType,

  CASE pDwi.type
    WHEN 'win' then 5
    WHEN 'impression' then 6
    WHEN 'click' then 7
    WHEN 'conversion' then 8
    WHEN 'event' THEN (CASE pDwi.eventType
      WHEN 1 THEN 6
      WHEN 2 THEN 7
      WHEN 3 THEN 7
      WHEN 4 THEN 7
      ELSE null END
    )
    ELSE null END
  AS dataType,

  tDwi.timestamp,
  tDwi.supply_bd_id,
  tDwi.supply_am_id,
  tDwi.supply_id,
  tDwi.supply_protocol,
  tDwi.request_flag,

  tDwi.ad_format,
  tDwi.site_app_id,
  tDwi.placement_id,
  tDwi.position,

  tDwi.country,
  tDwi.state,
  tDwi.city,

  tDwi.carrier,

  tDwi.os,
  tDwi.os_version,

  tDwi.device_type,
  tDwi.device_brand,
  tDwi.device_model,

  tDwi.age,
  tDwi.gender,

  tDwi.cost_currency,

  tDwi.demand_bd_id,
  tDwi.demand_am_id,
  tDwi.demand_id,

  tDwi.demand_seat_id,
  tDwi.demand_campaign_id,
  tDwi.demand_protocol,
  tDwi.target_site_app_id,
  tDwi.revenue_currency,

  tDwi.bid_price_model,
  tDwi.traffic_type,
  tDwi.currency,
  tDwi.supplyBidId,
  tDwi.bidRequestId,
--   pDwi.bidid AS performanceBidRequestId,

  tDwi.bundle,
  tDwi.size,

  0  AS supply_request_count,
--   待删
  0  AS supply_invalid_request_count,
  0  AS supply_bid_count,
  0. AS supply_bid_price_cost_currency,
  0. AS supply_bid_price,

  CASE pDwi.type
    WHEN 'win' THEN 1
    ELSE 0 END
  AS supply_win_count,

  CASE pDwi.type
    WHEN 'win' THEN (
     CASE
       WHEN (!pDwi.withPrice OR (pDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price_cost_currency
       ELSE (CASE
         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
         ELSE winDwi.price*1.00000000 END
       ) END
    )
    ELSE 0. END
  AS supply_win_price_cost_currency,

  CASE pDwi.type
    WHEN 'win' THEN (
     CASE
       WHEN (!winDwi.withPrice OR (winDwi.withPrice AND winDwi.price is null)) THEN tDwi.supply_bid_price
       ELSE (CASE
         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
           WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
           ELSE winDwi.price*1.000000 END
         )
         ELSE (CASE
           WHEN pDwi.cur = tDwi.currency THEN winDwi.price
           ELSE winDwi.price*1.000000 END
         ) END
       ) END
    )
    ELSE 0. END
  AS supply_win_price,

  0  as demand_request_count,
  0  as demand_bid_count,
  0. as demand_bid_price_revenue_currency,
  0. as demand_bid_price,
--   0  as demand_win_count,
--   0. as demand_win_price_revenue_currency,
--   0. as demand_win_price,

  CASE pDwi.type
    WHEN 'win' THEN 1
    ELSE 0 END
  AS demand_win_count,

  CASE pDwi.type
    WHEN 'win' THEN tDwi.demand_win_price_revenue_currency
    ELSE 0. END
  AS demand_win_price_revenue_currency,

  CASE pDwi.type
    WHEN 'win' THEN tDwi.demand_win_price
    ELSE 0. END
  AS demand_win_price,

-- 待删
  0  as demand_timeout_count,

  CASE pDwi.type
    WHEN 'impression' THEN 1
    WHEN 'event' THEN (CASE pDwi.eventType
      WHEN 1 THEN 1
      ELSE 0 END
    )
    ELSE 0 END
  AS impression_count,

  CASE pDwi.type
    WHEN 'impression' THEN (CASE
      WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price_cost_currency
      ELSE (CASE
        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN winDwi.price
        ELSE winDwi.price*1.0000000 END
      )END
    )
    ELSE 0. END
  AS impression_cost_currency,

  CASE pDwi.type
    WHEN 'impression' THEN (CASE
      WHEN !winDwi.withPrice OR winDwi.price is null THEN tDwi.supply_bid_price
      ELSE (CASE
        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
          WHEN tDwi.cost_currency = tDwi.currency THEN winDwi.price
          ELSE winDwi.price*1.000000 END
        )
        ELSE (CASE
          WHEN pDwi.cur = tDwi.currency THEN winDwi.price
          ELSE winDwi.price*1.0000000 END
        ) END
      )END
    )
    ELSE 0. END
  AS impression_cost,

  CASE pDwi.type
    WHEN 'impression' THEN tDwi.demand_win_price_revenue_currency
    ELSE 0. END
  AS impression_revenue_currency,

  CASE pDwi.type
    WHEN 'impression' THEN tDwi.demand_win_price
    ELSE 0. END
  AS impression_revenue,

  CASE pDwi.type
    WHEN 'click' THEN 1
    WHEN 'event' THEN (CASE pDwi.eventType
      WHEN 2 THEN 1
      WHEN 3 THEN 1
      WHEN 4 THEN 1
      ELSE 0 END
    )
    ELSE 0 END
  AS click_count,

  0. as click_cost_currency,
  0. as click_cost,
  0. as click_revenue_currency,
  0. as click_revenue,

  CASE pDwi.type
    WHEN 'conversion' THEN 1
    ELSE 0 END
  AS conversion_count,

  0. as conversion_price,

  0 as saveCount,

  tDwi.bidfloor as bidfloor,
  tDwi.site_id as site_id,
  tDwi.site_cat as site_cat,
  tDwi.site_domain as site_domain,
  tDwi.publisher_id as publisher_id,
  tDwi.app_id as app_id,
  tDwi.tmax as tmax,
  tDwi.ip as ip,
  tDwi.crid as crid,
  tDwi.cid as cid,
--   待删
  cast(null as string) as tips,
  pDwi.node as node,
  pDwi.tip_type,
  pDwi.tip_desc,
  tDwi.adm,

  CASE pDwi.type
    WHEN 'event' THEN 1
    ELSE 0 END
  AS event_count,
  tDwi.ssp_token,
  tDwi.rtb_version as rtb_version,
  tDwi.demand_using_time as demand_using_time,
  tDwi.adx_using_time as adx_using_time,
  tDwi.rater_type as rater_type,
  tDwi.rater_id as rater_id,
  tDwi.adomain as adomain,
  tDwi.media_type as media_type,


  pDwi.repeated,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:00:00') as l_time,
  pDwi.b_date,
  pDwi.b_time,
  pDwi.b_version as b_version

from (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time >= ${start_b_time} and b_time <= ${end_b_time}
) pDwi
left join (
  select * from nadx_traffic_dwi_40000 where dataType = 4 AND b_time >= ${start_b_time} and b_time <= ${end_b_time}
) tDwi ON tDwi.b_time = pDwi.b_time AND pDwi.bidid = tDwi.bidRequestId
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time >= ${start_b_time} and b_time <= ${end_b_time} AND type = 'win'
) winDwi ON winDwi.b_time = pDwi.b_time AND winDwi.bidid = pDwi.bidid;


set log = "MAKE nadx_performance_matched_dwi_tmp DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

-- select count(1) from nadx_performance_matched_dwi_tmp; 243488
-- select count(1) from nadx_performance_dwi where repeated = 'N' AND b_time = ${b_time}
-- select count(distinct(dataType, bidRequestId)) from nadx_performance_matched_dwi_tmp; 243130
-- select count(1) from nadx_performance_matched_dwi_tmp_unrepeated; 243130
-- 去重

set log = "MAKE nadx_performance_matched_dwi_tmp_unrepeated START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

drop table if exists nadx_performance_matched_dwi_tmp_unrepeated;
create table nadx_performance_matched_dwi_tmp_unrepeated as
select
  repeats                          ,
  rowkey                            ,

  dataType                          ,
  `timestamp`                       ,
  supply_bd_id                      ,
  supply_am_id                      ,
  supply_id                         ,
  supply_protocol                   ,
  request_flag                      ,

  ad_format                         ,
  site_app_id                       ,
  placement_id                      ,
  position                          ,

  country                           ,
  state                             ,
  city                              ,

  carrier                           ,

  os                                ,
  os_version                        ,

  device_type                       ,
  device_brand                      ,
  device_model                      ,

  age                               ,
  gender                            ,

  cost_currency                     ,

-- demand
  demand_bd_id                      ,
  demand_am_id                      ,
  demand_id                         ,

-- destination
  demand_seat_id                    ,
  demand_campaign_id                ,
  demand_protocol                   ,
  target_site_app_id                ,
  revenue_currency                  ,

-- common
  bid_price_model                   ,
  traffic_type                      ,
  currency                          ,

-- id
  supplyBidId                       ,
  bidRequestId                      ,

  bundle                            ,
  size                              ,

  supply_request_count              ,
--   待删，冗余
  supply_invalid_request_count      ,
  supply_bid_count                  ,
  supply_bid_price_cost_currency    ,
  supply_bid_price                  ,
  supply_win_count                  ,
  supply_win_price_cost_currency    ,
  supply_win_price                  ,

  demand_request_count              ,
  demand_bid_count                  ,
  demand_bid_price_revenue_currency ,
  demand_bid_price                  ,
  demand_win_count                  ,
  demand_win_price_revenue_currency ,
  demand_win_price                  ,
  demand_timeout_count              ,

  impression_count                  ,
  impression_cost_currency          ,
  impression_cost                   ,
  impression_revenue_currency       ,
  impression_revenue                ,
  click_count                       ,
  click_cost_currency               ,
  click_cost                        ,
  click_revenue_currency            ,
  click_revenue                     ,
  conversion_count                  ,
  conversion_price                  ,
  saveCount                         ,
  bidfloor                          ,
  site_id                           ,
  site_cat                          ,
  site_domain                       ,
  publisher_id                      ,
  app_id                            ,
  tmax                              ,
  ip                                ,
  crid                              ,
  cid                               ,
  tips                              ,
  node                              ,
  tip_type                          ,
  tip_desc                          ,
  adm                               ,

  event_count                       ,
  ssp_token                         ,
  rtb_version                       ,
  demand_using_time                 ,
  adx_using_time                    ,
  rater_type                        ,
  rater_id                          ,
  cast(null as STRING) as raterType ,
  cast(null as STRING) as raterId   ,
  adomain                           ,
  media_type                        ,

  repeated                          ,
  l_time                            ,
  b_date                            ,
  b_time                            ,
  b_version
from(
  select
    *,
    row_number() over(partition by dataType, bidRequestId order by 1 desc) row_num
  from nadx_performance_matched_dwi_tmp
)
where row_num = 1;

set log = "MAKE nadx_performance_matched_dwi_tmp_unrepeated DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
----------------------------------------------------------------------------------------------------
-- Drop Partition
----------------------------------------------------------------------------------------------------
-- alter table nadx_overall_dwr drop if exists partition(b_date=${b_date});
-- alter table nadx_overall_dwr drop partition(b_time=${start_b_time});

set log = "MAKE nadx_overall_dwr START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

insert overwrite table nadx_overall_dwr
select
  supply_bd_id                      ,
  supply_am_id                      ,
  supply_id                         ,
  supply_protocol                   ,
  request_flag                      ,
  ad_format                         ,
  site_app_id as site_app_id        ,
  null as placement_id              ,
  position                          ,
  country                           ,
  state                             ,
  null as city                      ,
  null as carrier                   ,
  os                                ,
  null as os_version                ,
  device_type                       ,
  null as device_brand              ,
  null as device_model              ,
  age                               ,
  gender                            ,
  cost_currency                     ,
  demand_bd_id                      ,
  demand_am_id                      ,
  demand_id                         ,
  demand_seat_id                    ,
  demand_campaign_id                ,
  demand_protocol                   ,
  target_site_app_id                ,
  revenue_currency                  ,
  bid_price_model                   ,
  traffic_type                      ,
  currency                          ,
  null as bundle                  ,
  size                              ,

  sum(supply_request_count)              as supply_request_count,
--   待删，冗余
  sum(supply_invalid_request_count)      as supply_invalid_request_count,
  sum(supply_bid_count)                  as supply_bid_count,
  sum(supply_bid_price_cost_currency    / IF(bid_price_model = 1, 1000.0, 1.0)) as supply_bid_price_cost_currency,
  sum(supply_bid_price                  / IF(bid_price_model = 1, 1000.0, 1.0)) as supply_bid_price,
  sum(supply_win_count)                  as supply_win_count,
  sum(supply_win_price_cost_currency    / IF(bid_price_model = 1, 1000.0, 1.0)) as supply_win_price_cost_currency,
  sum(supply_win_price                  / IF(bid_price_model = 1, 1000.0, 1.0)) as supply_win_price,

  sum(demand_request_count)              as demand_request_count,
  sum(demand_bid_count)                  as demand_bid_count,
  sum(demand_bid_price_revenue_currency / IF(bid_price_model = 1, 1000.0, 1.0)) as demand_bid_price_revenue_currency,
  sum(demand_bid_price                  / IF(bid_price_model = 1, 1000.0, 1.0)) as demand_bid_price,
  sum(demand_win_count)                  as demand_win_count,
  sum(demand_win_price_revenue_currency / IF(bid_price_model = 1, 1000.0, 1.0)) as demand_win_price_revenue_currency,
  sum(demand_win_price                  / IF(bid_price_model = 1, 1000.0, 1.0)) as demand_win_price,
  sum(demand_timeout_count)              as demand_timeout_count,

  sum(impression_count)                  as impression_count,
  sum(impression_cost_currency          / IF(bid_price_model = 1, 1000.0, 1.0)) as impression_cost_currency,
  sum(impression_cost                   / IF(bid_price_model = 1, 1000.0, 1.0)) as impression_cost,
  sum(impression_revenue_currency       / IF(bid_price_model = 1, 1000.0, 1.0)) as impression_revenue_currency,
  sum(impression_revenue                / IF(bid_price_model = 1, 1000.0, 1.0)) as impression_revenue,
  sum(click_count)                       as click_count,
  sum(click_cost_currency)               as click_cost_currency,
  sum(click_cost)                        as click_cost,
  sum(click_revenue_currency)            as click_revenue_currency,
  sum(click_revenue)                     as click_revenue,
  sum(conversion_count)                  as conversion_count,
  sum(conversion_price)                  as conversion_price,

-- 待删，用tip_type
  null                                   as tips,
  null                                   as node,
  tip_type                               as tip_type,

  null                                   as tip_desc,
  sum(event_count)                       as event_count,
  null                                   as ssp_token,
  rtb_version                            as rtb_version,
  demand_using_time                      as demand_using_time,
  adx_using_time                         as adx_using_time,

  null                                   as site_domain,
  null                                   as publisher_id,
  null                                   as raterType,
  null                                   as raterId,
  null                                   as adomain,
  null                                   as crid,
  null                                   as bidfloor,
  rater_type                             as rater_type,
  rater_id                               as rater_id,
  media_type                             as media_type,

  from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') as l_time,
  b_date,
  b_time,
  '0' as b_version

from (
  select * from nadx_performance_matched_dwi_tmp_unrepeated where b_time >= ${start_b_time} and b_time <= ${end_b_time}
  UNION ALL
  select * from nadx_traffic_dwi where b_time >= ${start_b_time} and b_time <= ${end_b_time}
)
group by
  supply_bd_id                      ,
  supply_am_id                      ,
  supply_id                         ,
  supply_protocol                   ,
  request_flag                      ,
  ad_format                         ,
  site_app_id                       ,
--placement_id                      ,
  position                          ,
  country                           ,
  state                             ,
--city                              ,
--carrier                           ,
  os                                ,
--os_version                        ,
  device_type                       ,
--device_brand                      ,
--device_model                      ,
  age                               ,
  gender                            ,
  cost_currency                     ,
  demand_bd_id                      ,
  demand_am_id                      ,
  demand_id                         ,
  demand_seat_id                    ,
  demand_campaign_id                ,
  demand_protocol                   ,
  target_site_app_id                ,
  revenue_currency                  ,
  bid_price_model                   ,
  traffic_type                      ,
  currency                          ,
--   bundle                            ,
  size                              ,
  b_date                            ,
  b_time                            ,
--   node                              ,
  rtb_version,
  demand_using_time,
  adx_using_time,
  tip_type,
  rater_type,
  rater_id,
  media_type
--   ,
--   tip_desc                          ,
--   ssp_token
;

set log = "MAKE nadx_overall_dwr done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";


-- site_app_id,
-- placement_id,
-- city,
-- carrier,
-- os_version,
-- device_brand,
-- device_model,
-- bundle