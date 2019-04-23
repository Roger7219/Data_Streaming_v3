CREATE TABLE nadx_traffic_dwi(
  repeats                           int,
  rowkey                            string,

  dataType                          int,
  `timestamp`                       bigint,
  supply_bd_id                      int,
  supply_am_id                      int,
  supply_id                         int,
  supply_protocol                   int,
  request_flag                      int,

  ad_format                         int,
  site_app_id                       int,
  placement_id                      int,
  position                          int,

  country                           string,
  state                             string,
  city                              string,

  carrier                           string,

  os                                string,
  os_version                        string,

  device_type                       int,
  device_brand                      string,
  device_model                      string,

  age                               string,
  gender                            string,

  cost_currency                     string,

-- demand
  demand_bd_id                      int,
  demand_am_id                      int,
  demand_id                         int,

-- destination
  demand_seat_id                    string,
  demand_campaign_id                string,
  demand_protocol                   int,
  target_site_app_id                string,
  revenue_currency                  string,

-- common
  bid_price_model                   int,
  traffic_type                      int,
  currency                          string,

-- id
  supplyBidId                       string,
  bidRequestId                      string,

  bundle                            string,
  size                              string,

  supply_request_count              bigint,
  supply_invalid_request_count      bigint,
  supply_bid_count                  bigint,
  supply_bid_price_cost_currency    double,
  supply_bid_price                  double,
  supply_win_count                  bigint,
  supply_win_price_cost_currency    double,
  supply_win_price                  double,

  demand_request_count              bigint,
  demand_bid_count                  bigint,
  demand_bid_price_revenue_currency double,
  demand_bid_price                  double,
  demand_win_count                  bigint,
  demand_win_price_revenue_currency double,
  demand_win_price                  double,
  demand_timeout_count              bigint,

  impression_count                  bigint,
  impression_cost_currency          double,
  impression_cost                   double,
  impression_revenue_currency       double,
  impression_revenue                double,
  click_count                       bigint,
  click_cost_currency               double,
  click_cost                        double,
  click_revenue_currency            double,
  click_revenue                     double,
  conversion_count                  bigint,
  conversion_price                  double,
  saveCount                         int
  
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

CREATE TABLE nadx_performance_dwi(
  repeats   int,
  rowkey    string,

  `type`    string,
  bidTime   bigint, -- unix timestamp in second
  supplyid  int,
  bidid     string,
  impid     string,
  price     double,
  cur       string,
  withPrice boolean,
  eventType int
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;



-----------------------------------------------------------------------------------------
-- 离线统计
-----------------------------------------------------------------------------------------
-- spark-sql  --driver-memory  8G   --executor-memory 8G
-- beeline -n "" -p ""  -u jdbc:hive2://master:10016/default --outputformat=table --verbose=true

set hive.exec.dynamic.partition.mode=nonstrict;
set spark.default.parallelism = 4;
set spark.sql.shuffle.partitions = 4;
set b_time = "2019-03-24 11:00:00";

-- 376490
-- 243130
drop table if exists nadx_performance_matched_dwi_tmp;
create table nadx_performance_matched_dwi_tmp as
select
  pDwi.repeats,
  pDwi.rowkey,

--   pDwi.type      AS type,
--   pDwi.bidTime   AS bidTime,
--   pDwi.supplyid  AS supplyid,
--   pDwi.bidid     AS bidid,
--   pDwi.impid     AS impid,
--   pDwi.price     AS price,
--   pDwi.cur       AS cur,
--   pDwi.withPrice AS withPrice,
--   pDwi.eventType AS eventType,

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

  0 AS supply_request_count,
  0 AS supply_invalid_request_count,
  0 AS supply_bid_count,
  tDwi.supply_bid_price_cost_currency,
  tDwi.supply_bid_price,

  CASE pDwi.type
    WHEN 'win' THEN 1
    WHEN 'impression' THEN 0
    WHEN 'click' THEN 0
    WHEN 'conversion' THEN 0
    WHEN 'event' THEN tDwi.dataType
    ELSE 0 END
  AS supply_win_count,

  CASE pDwi.type
    WHEN 'win' THEN (
     CASE
       WHEN (!pDwi.withPrice OR (pDwi.withPrice AND pDwi.price is null)) THEN tDwi.supply_bid_price_cost_currency
       ELSE (CASE
         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN pDwi.price
         ELSE pDwi.price*1.00000000 END
       ) END
    )
    ELSE 0. END
  AS supply_win_price_cost_currency,

  CASE pDwi.type
    WHEN 'win' THEN (
     CASE
       WHEN (!pDwi.withPrice OR (pDwi.withPrice AND pDwi.price is null)) THEN tDwi.supply_bid_price
       ELSE (CASE
         WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
           WHEN tDwi.cost_currency = tDwi.currency THEN pDwi.price
           ELSE pDwi.price*1.000000 END
         )
         ELSE (CASE
           WHEN pDwi.cur = tDwi.currency THEN pDwi.price
           ELSE pDwi.price*1.000000 END
         ) END
       ) END
    )
    ELSE 0. END
  AS supply_win_price,

  0  as demand_request_count,
  0  as demand_bid_count,
  0. as demand_bid_price_revenue_currency,
  0. as demand_bid_price,
  0  as demand_win_count,
  0. as demand_win_price_revenue_currency,
  tDwi.demand_win_price,
  0  as demand_timeout_count,

  CASE pDwi.type
    WHEN 'impression' THEN 1
    WHEN 'event' THEN (CASE pDwi.eventType
      WHEN 1 THEN 1
      ELSE 0 END
    )
    ELSE 0 END
  AS impression_count,

  0. as impression_cost_currency,

  CASE pDwi.type
    WHEN 'impression' THEN (CASE
      WHEN !pDwi.withPrice OR pDwi.price is null THEN tDwi.supply_win_price/IF(tDwi.bid_price_model = 1, 1000.0, 1.0)
      ELSE (CASE
        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency THEN (CASE
          WHEN tDwi.cost_currency = tDwi.currency THEN pDwi.price/IF(tDwi.bid_price_model = 1, 1000.0, 1.0)
          ELSE pDwi.price*1.000000/IF(tDwi.bid_price_model = 1, 1000.0, 1.0) END
        )
        ELSE (CASE
          WHEN pDwi.cur = tDwi.currency THEN pDwi.price/IF(tDwi.bid_price_model = 1, 1000.0, 1.0)
          ELSE pDwi.price*1.0000000/IF(tDwi.bid_price_model = 1, 1000.0, 1.0) END
        ) END
      )END
    )
    ELSE 0. END
  AS impression_cost,

  CASE pDwi.type
    WHEN 'impression' THEN (CASE
      WHEN !pDwi.withPrice OR pDwi.price is null THEN tDwi.supply_win_price_cost_currency/IF(tDwi.bid_price_model = 1, 1000.0, 1.0)
      ELSE (CASE
        WHEN pDwi.cur is null OR pDwi.cur = tDwi.cost_currency/IF(tDwi.bid_price_model = 1, 1000.0, 1.0) THEN pDwi.price
        ELSE pDwi.price*1.0000000/IF(tDwi.bid_price_model = 1, 1000.0, 1.0) END
      ) END
    )
    ELSE 0. END
  AS impression_revenue_currency,

  CASE pDwi.type
    WHEN 'impression' THEN tDwi.demand_win_price/IF(tDwi.bid_price_model = 1, 1000.0, 1.0)
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

  pDwi.repeated,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:00:00') as l_time,
  pDwi.b_date,
  pDwi.b_time,
  pDwi.b_version as b_version

from (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = ${b_time}
) pDwi
left join (
  select * from nadx_traffic_dwi where dataType = 4 AND b_time = ${b_time}
) tDwi ON tDwi.b_time = pDwi.b_time AND pDwi.bidid = tDwi.bidRequestId;

-- select count(1) from nadx_performance_matched_dwi_tmp; 243488
-- select count(1) from nadx_performance_dwi where repeated = 'N' AND b_time = ${b_time}
-- select count(distinct(dataType, bidRequestId)) from nadx_performance_matched_dwi_tmp; 243130
-- select count(1) from nadx_performance_matched_dwi_tmp_unrepeated; 243130
-- 去重
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

-- set b_time = "2019-03-24 11:00:00";
-- select count(distinct (dataType, bidRequestId)) from nadx_performance_dwi where repeated = 'N' and b_time = ${b_time}

-- hadoop fs -ls -R -h /apps/hive/warehouse/nadx_performance_matched_dwi_tmp_unrepeated/

select * from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00' limit 100;
-- select count(distinct supply_bd_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct supply_am_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct supply_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct supply_protocol) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct request_flag) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct ad_format) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct site_app_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct placement_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct position) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct country) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct state) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct city) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct carrier) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct os) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct os_version) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct device_type) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct device_brand) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct device_model) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct age) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct gender) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct cost_currency) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct demand_bd_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct demand_seat_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct demand_campaign_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct demand_protocol) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct target_site_app_id) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct revenue_currency) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct bid_price_model) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct traffic_type) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct currency) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct bundle) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';
-- select count(distinct size) from nadx_overall_dwr  where b_time ='2019-03-24 11:00:00';

alter table nadx_overall_dwr drop partition(b_time=${b_time});
insert overwrite table nadx_overall_dwr
select
  supply_bd_id                      ,
  supply_am_id                      ,
  supply_id                         ,
  supply_protocol                   ,
  request_flag                      ,
  ad_format                         ,
  null as site_app_id               ,
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
  null as bundle                    ,
  size                              ,

  sum(supply_request_count)              as supply_request_count,
  sum(supply_invalid_request_count)      as supply_invalid_request_count,
  sum(supply_bid_count)                  as supply_bid_count,
  sum(supply_bid_price_cost_currency)    as supply_bid_price_cost_currency,
  sum(supply_bid_price)                  as supply_bid_price,
  sum(supply_win_count)                  as supply_win_count,
  sum(supply_win_price_cost_currency)    as supply_win_price_cost_currency,
  sum(supply_win_price)                  as supply_win_price,

  sum(demand_request_count)              as demand_request_count,
  sum(demand_bid_count)                  as demand_bid_count,
  sum(demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
  sum(demand_bid_price)                  as demand_bid_price,
  sum(demand_win_count)                  as demand_win_count,
  sum(demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
  sum(demand_win_price)                  as demand_win_price,
  sum(demand_timeout_count)              as demand_timeout_count,

  sum(impression_count)                  as impression_count,
  sum(impression_cost_currency)          as impression_cost_currency,
  sum(impression_cost)                   as impression_cost,
  sum(impression_revenue_currency)       as impression_revenue_currency,
  sum(impression_revenue)                as impression_revenue,
  sum(click_count)                       as click_count,
  sum(click_cost_currency)               as click_cost_currency,
  sum(click_cost)                        as click_cost,
  sum(click_revenue_currency)            as click_revenue_currency,
  sum(click_revenue)                     as click_revenue,
  sum(conversion_count)                  as conversion_count,
  sum(conversion_price)                  as conversion_price,

  from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') as l_time,
  b_date,
  b_time,
  '0' as b_version

from (
  select * from nadx_performance_matched_dwi_tmp where b_time = ${b_time}
  UNION ALL
  select * from nadx_traffic_dwi where b_time = ${b_time}
)
group by
  supply_bd_id                      ,
  supply_am_id                      ,
  supply_id                         ,
  supply_protocol                   ,
  request_flag                      ,
  ad_format                         ,
--site_app_id                       ,
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
--bundle                            ,
  size                              ,
  b_date                            ,
  b_time ;


-- site_app_id,
-- placement_id,
-- city,
-- carrier,
-- os_version,
-- device_brand,
-- device_model,
-- bundle
--------2019-04-18  lilei start
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (bidfloor STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (site_id STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (site_cat STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (site_domain STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (publisher_id STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (app_id STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (tmax STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (ip STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (crid STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (cid STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (tips STRING);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (node STRING);
--------2019-04-18  lilei end

-- 2019-4-23 Tip功能
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (tip_type int);
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (tip_desc STRING);
-- 2019-4-23 geo送检功能涉及
ALTER TABLE nadx_traffic_dwi ADD COLUMNS (adm STRING);

-- 效果数据表
-- 2019-4-23
ALTER TABLE nadx_performance_dwi ADD COLUMNS (node STRING);
ALTER TABLE nadx_performance_dwi ADD COLUMNS (tip_type int);
ALTER TABLE nadx_performance_dwi ADD COLUMNS (tip_desc STRING);

-- dwr 表
-- tip_type
ALTER TABLE nadx_overall_dwr ADD COLUMNS (tip_type STRING);

