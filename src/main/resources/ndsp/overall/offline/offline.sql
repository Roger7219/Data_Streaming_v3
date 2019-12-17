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

set log = "MAKE ndsp_performance_matched_dwi_tmp START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

drop table if exists ndsp_performance_matched_dwi_tmp;
create  table ndsp_performance_matched_dwi_tmp as
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

  tDwi.`timestamp`,
  tDwi.supply_id,
  tDwi.supply_protocol,
  tDwi.request_flag,
  tDwi.request_status,

  tDwi.ad_format,
  tDwi.site_app_id,
  tDwi.placement_id,
  tDwi.position,

  tDwi.country,
  tDwi.region,
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

  tDwi.proxy_id,
  tDwi.mediabuy_id,
  tDwi.bd_id,

  tDwi.am_id,
  tDwi.campaign_id,
  tDwi.ad_id,
  tDwi.revenue_currency,

  tDwi.bid_price_model,
  tDwi.traffic_type,
  tDwi.currency,
  tDwi.supplyBidId,
  tDwi.bidRequestId,
  tDwi.bundle,
  tDwi.size,

  0  AS supply_request_count,
  0  AS supply_bid_count,
  0. AS supply_bid_price_cost_currency,
  0. AS supply_bid_price,
  0. AS adver_bid_price_cost_currency,
  0. AS adver_bid_price,

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
    WHEN 'impression' THEN tDwi.supply_bid_price
    ELSE 0. END
  AS impression_revenue_currency,

  CASE pDwi.type
    WHEN 'impression' THEN tDwi.supply_bid_price
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


  pDwi.repeated,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:00:00') as l_time,
  pDwi.b_date,
  pDwi.b_time,
  pDwi.b_version as b_version

from (
  select * from ndsp_performance_dwi where repeated = 'N' AND b_time >= ${start_b_time} and b_time <= ${end_b_time}
) pDwi
left join (
  select * from ndsp_traffic_dwi where dataType = 1 AND b_time >= ${start_b_time} and b_time <= ${end_b_time}
) tDwi ON tDwi.b_time = pDwi.b_time AND pDwi.bidid = tDwi.bidRequestId
left join (
  select * from ndsp_performance_dwi where repeated = 'N' AND b_time >= ${start_b_time} and b_time <= ${end_b_time} AND type = 'win'
) winDwi ON winDwi.b_time = pDwi.b_time AND winDwi.bidid = pDwi.bidid;


set log = "MAKE ndsp_performance_matched_dwi_tmp DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

-- select count(1) from ndsp_performance_matched_dwi_tmp; 243488
-- select count(1) from ndsp_performance_dwi where repeated = 'N' AND b_time = ${b_time}
-- select count(distinct(dataType, bidRequestId)) from ndsp_performance_matched_dwi_tmp; 243130
-- select count(1) from ndsp_performance_matched_dwi_tmp_unrepeated; 243130
-- 去重

set log = "MAKE ndsp_performance_matched_dwi_tmp_unrepeated START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

drop table if exists ndsp_performance_matched_dwi_tmp_unrepeated;
create table ndsp_performance_matched_dwi_tmp_unrepeated as
select
repeats                           ,
rowkey                            ,

dataType                         ,
`timestamp`                      ,
supply_id                        ,
supply_protocol                  ,
request_flag                     ,
request_status                     ,

ad_format                         ,
site_app_id                       ,
placement_id                      ,
position                          ,

country                           ,
region                             ,
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
proxy_id                      ,
mediabuy_id                      ,
bd_id                         ,
am_id                         ,
campaign_id                         ,
ad_id                         ,

-- destination
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
supply_bid_count                  ,
supply_bid_price_cost_currency    ,
supply_bid_price                  ,
supply_win_count                  ,
supply_win_price_cost_currency    ,
supply_win_price                  ,
adver_bid_price_cost_currency    ,
adver_bid_price                  ,

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

repeated                          ,
l_time                            ,
b_date                            ,
b_time                            ,
b_version
from(
select
*,
row_number() over(partition by dataType, bidRequestId order by 1 desc) row_num
from ndsp_performance_matched_dwi_tmp
) z
where row_num=1;

set log = "MAKE ndsp_performance_matched_dwi_tmp_unrepeated DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
----------------------------------------------------------------------------------------------------
-- Drop Partition
----------------------------------------------------------------------------------------------------
-- alter table ndsp_overall_dwr drop if exists partition(b_date=${b_date});
-- alter table ndsp_overall_dwr drop partition(b_time=${start_b_time});

set log = "MAKE ndsp_overall_dwr START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";


insert overwrite table ndsp_overall_dwr
select
supply_id,
supply_protocol,
request_flag,
request_status,
ad_format,
site_app_id,
placement_id,
position,
country,
region,
city,
carrier,
os,
os_version,
device_type,
device_brand,
device_model,
age,
gender,
cost_currency,
proxy_id,
mediabuy_id,
bd_id,
am_id,
campaign_id,
ad_id,
revenue_currency,
bid_price_model,
traffic_type,
currency,
bundle,
size,
sum(supply_request_count)                                                     as  supply_request_count,
sum(supply_bid_count)                                                         as  supply_bid_count,
sum(supply_bid_price_cost_currency/ IF(bid_price_model = 1, 1000.0, 1.0) )    as  supply_bid_price_cost_currency,
sum(supply_bid_price/ IF(bid_price_model = 1, 1000.0, 1.0) )                  as  supply_bid_price,
sum(supply_win_count)                                                         as  supply_win_count,
sum(supply_win_price_cost_currency/ IF(bid_price_model = 1, 1000.0, 1.0) )    as  supply_win_price_cost_currency,
sum(supply_win_price/ IF(bid_price_model = 1, 1000.0, 1.0) )                  as  supply_win_price,
sum(impression_count)                                                         as  impression_count,
sum(impression_cost_currency/ IF(bid_price_model = 1, 1000.0, 1.0) )          as  impression_cost_currency,
sum(impression_cost/ IF(bid_price_model = 1, 1000.0, 1.0) )                   as  impression_cost,
sum(impression_revenue_currency/ IF(bid_price_model = 1, 1000.0, 1.0) )       as  impression_revenue_currency,
sum(impression_revenue/ IF(bid_price_model = 1, 1000.0, 1.0) )                as  impression_revenue,
sum(click_count)                                                              as  click_count,
sum(click_cost_currency)                                                      as  click_cost_currency,
sum(click_cost)                                                               as  click_cost,
sum(click_revenue_currency)                                                   as  click_revenue_currency,
sum(click_revenue)                                                            as  click_revenue,
sum(conversion_count)                                                         as  conversion_count,
sum(conversion_price)                                                         as  conversion_price,
from_unixtime(unix_timestamp(),'yyyy-MM-dd 00:00:00') as l_time,
b_date,
b_time,
'0' as b_version
from (
  select * from ndsp_performance_matched_dwi_tmp_unrepeated where b_time >= ${start_b_time} and b_time <= ${end_b_time}
  UNION ALL
  select * from ndsp_traffic_dwi where b_time >= ${start_b_time} and b_time <= ${end_b_time}
) z
group by
supply_id,
supply_protocol,
request_flag,
request_status,
ad_format,
site_app_id,
placement_id,
position,
country,
region,
city,
carrier,
os,
os_version,
device_type,
device_brand,
device_model,
age,
gender,
cost_currency,
proxy_id,
mediabuy_id,
bd_id,
am_id,
campaign_id,
ad_id,
revenue_currency,
bid_price_model,
traffic_type,
currency,
bundle,
size,
b_date,
b_time;

set log = "MAKE ndsp_overall_dwr DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
