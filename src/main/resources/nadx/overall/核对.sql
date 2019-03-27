select
    dataType,
    sum(supply_request_count) as supply_request_count,
    sum(supply_invalid_request_count) as supply_invalid_request_count,
    sum(supply_bid_count) as supply_bid_count,
    sum(supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
    sum(supply_bid_price) as supply_bid_price,
    sum(supply_win_price_cost_currency) as supply_win_price_cost_currency,
    sum(supply_win_price) as supply_win_price,
    sum(demand_request_count) as demand_request_count,
    sum(demand_bid_count) as demand_bid_count,
    sum(demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
    sum(demand_bid_price) as demand_bid_price,
    sum(demand_win_count) as demand_win_count,
    sum(demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
    sum(demand_win_price) as demand_win_price,
    sum(demand_timeout_count) as demand_timeout_count,
    sum(impression_count) as impression_count,
    sum(impression_cost_currency) as impression_cost_currency,
    sum(impression_cost) as impression_cost,
    sum(impression_revenue_currency) as impression_revenue_currency,
    sum(impression_revenue) as impression_revenue,
    sum(click_count) as click_count,
    sum(click_cost_currency) as click_cost_currency,
    sum(click_cost) as click_cost,
    sum(click_revenue_currency) as click_revenue_currency,
    sum(click_revenue) as click_revenue,
    sum(conversion_count) as conversion_count,
    sum(conversion_price) as conversion_price
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00'
group by dataType

----

select
  count(1), bidRequestId
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 2
group by bidRequestId
order by 1 desc
limit 100

----------
select count(distinct bidRequestId)
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 2

----------
select count(distinct bidRequestId)
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 2

----
select count(distinct bidRequestId)
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 3
-----
select count(distinct bidRequestId)
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 3
------

select count(bidRequestId)
from nadx_traffic_dwi
where
demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 3 and demand_bid_count>0

-- dataType =3 if demand_bid_count is null 则timeoutcount > 0
select * from nadx_traffic_dwi
where demand_id = 5 and b_time = '2019-03-20 03:00:00' and dataType = 3 and demand_bid_count is null  limit 1
----------------------

drop table nadx_joined_performance_dwi_dataType4;
create table nadx_joined_performance_dwi_dataType4 as
select
p.`type`                              p_type,
p.bidTime                             p_bidTime ,
p.supplyid                            p_supplyid,
p.bidid                               p_bidid,
p.impid                               p_impid,
p.price                               p_price,
p.cur                                 p_cur ,
p.withPrice                           p_withPrice,
p.eventType                           p_eventType,

t.  dataType                          t_dataType,
t.  `timestamp`                       t_timestamp,
t.  supply_bd_id                      t_supply_bd_id,
t.  supply_am_id                      t_supply_am_id,
t.  supply_id                         t_supply_id,
t.  supply_protocol                   t_supply_protocol,
t.  request_flag                      t_request_flag,

t.  ad_format                         t_ad_format,
t.  site_app_id                       t_site_app_id,
t.  placement_id                      t_placement_id,
t.  position                          t_position,

t.  country                           t_country,
t.  state                             t_state,
t.  city                              t_city,

t.  carrier                           t_carrier,

t.  os                                t_os,
t.  os_version                        t_os_version,

t.  device_type                       t_device_type,
t.  device_brand                      t_device_brand,
t.  device_model                      t_device_model,

t.  age                               t_age,
t.  gender                            t_gender,

t.  cost_currency                     t_cost_currency,

-- demand
t.  demand_bd_id                      t_demand_bd_id,
t.  demand_am_id                      t_demand_am_id,
t.  demand_id                         t_demand_id,

-- destination
t.  demand_seat_id                    t_demand_seat_id,
t.  demand_campaign_id                t_demand_campaign_id,
t.  demand_protocol                   t_demand_protocol,
t.  target_site_app_id                t_target_site_app_id,
t.  revenue_currency                  t_revenue_currency,

-- common
t.  bid_price_model                   t_bid_price_model,
t.  traffic_type                      t_traffic_type,
t.  currency                          t_currency,

-- id
t.  supplyBidId                       t_supplyBidId,
t.  bidRequestId                      t_bidRequestId,

t.  bundle                            t_bundle,
t.  size                              t_size,

t.  supply_request_count              t_supply_request_count,
t.  supply_invalid_request_count      t_supply_invalid_request_count,
t.  supply_bid_count                  t_supply_bid_count,
t.  supply_bid_price_cost_currency    t_supply_bid_price_cost_currency,
t.  supply_bid_price                  t_supply_bid_price,
t.  supply_win_count                  t_supply_win_count,
t.  supply_win_price_cost_currency    t_supply_win_price_cost_currency,
t.  supply_win_price                  t_supply_win_price,

t.  demand_request_count              t_demand_request_count,
t.  demand_bid_count                  t_demand_bid_count,
t.  demand_bid_price_revenue_currency t_demand_bid_price_revenue_currency,
t.  demand_bid_price                  t_demand_bid_price,
t.  demand_win_count                  t_demand_win_count,
t.  demand_win_price_revenue_currency t_demand_win_price_revenue_currency,
t.  demand_win_price                  t_demand_win_price,
t.  demand_timeout_count              t_demand_timeout_count,

t.  impression_count                  t_impression_count,
t.  impression_cost_currency          t_impression_cost_currency,
t.  impression_cost                   t_impression_cost,
t.  impression_revenue_currency       t_impression_revenue_currency,
t.  impression_revenue                t_impression_revenue,
t.  click_count                       t_click_count,
t.  click_cost_currency               t_click_cost_currency,
t.  click_cost                        t_click_cost,
t.  click_revenue_currency            t_click_revenue_currency,
t.  click_revenue                     t_click_revenue,
t.  conversion_count                  t_conversion_count,
t.  conversion_price                  t_conversion_price,
t.  saveCount                         t_saveCount,
t. b_time

from nadx_performance_dwi p
left join (
select * from nadx_traffic_dwi where b_time = '2019-03-20 04:00:00' and dataType = 4
) t on p.bidid = t.bidRequestId and p.b_time = t.b_time
where t.demand_id = 5  and p.b_time = '2019-03-20 04:00:00'


ex:     3-19 19:00
advent: 3-19 12:00
bi:     3-20 03:00



-----

select count(1) from nadx_traffic_dwi where b_time = '2019-03-20 03:00:00' and dataType = 4 and demand_id = 5
select count(distinct bidRequestId) from nadx_traffic_dwi where b_time = '2019-03-20 03:00:00' and dataType = 4 and demand_id = 5

----------


-----

select *
from nadx_joined_performance_dwi_dataType4
where

-------------------

select count(distinct bidid)
from nadx_performance_dwi
where  b_time = '2019-03-20 03:00:00' and  `type` ='impression'

-----
select count(bidid)
from nadx_performance_dwi
where  b_time = '2019-03-20 03:00:00' and  `type` ='impression'

------
-- nadx_performance_dwi 存在大量重复
select count(bidid), bidid
from nadx_performance_dwi
where  b_time = '2019-03-20 03:00:00' and  `type` ='impression'
group by bidid
order by 1 desc
limit 1000

---
select * from nadx_performance_dwi where bidid= '200032F4A9C436F1F5B4E01553022767909'

----
-- where t_demand_id = 5  and b_time = '2019-03-20 03:00:00'

-----
select count(1), p_type from nadx_joined_performance_dwi_dataType4
where t_demand_id = 5  and b_time = '2019-03-20 04:00:00'
group by p_type

---- = 3379
select count(1) from nadx_joined_performance_dwi_dataType4
where t_demand_id = 5  and b_time = '2019-03-20 03:00:00' and p_type ='impression'

select sum(t_impression_count) from nadx_joined_performance_dwi_dataType4
where t_demand_id = 5  and b_time = '2019-03-20 03:00:00' and p_type ='impression'
-------------
-- 去重
drop table nadx_joined_performance_dwi_dataType4_qu_chong;
create table nadx_joined_performance_dwi_dataType4_qu_chong as
select
p.`type`                              p_type,
p.bidTime                             p_bidTime ,
p.supplyid                            p_supplyid,
p.bidid                               p_bidid,
p.impid                               p_impid,
p.price                               p_price,
p.cur                                 p_cur ,
p.withPrice                           p_withPrice,
p.eventType                           p_eventType,

t.  dataType                          t_dataType,
t.  `timestamp`                       t_timestamp,
t.  supply_bd_id                      t_supply_bd_id,
t.  supply_am_id                      t_supply_am_id,
t.  supply_id                         t_supply_id,
t.  supply_protocol                   t_supply_protocol,
t.  request_flag                      t_request_flag,

t.  ad_format                         t_ad_format,
t.  site_app_id                       t_site_app_id,
t.  placement_id                      t_placement_id,
t.  position                          t_position,

t.  country                           t_country,
t.  state                             t_state,
t.  city                              t_city,

t.  carrier                           t_carrier,

t.  os                                t_os,
t.  os_version                        t_os_version,

t.  device_type                       t_device_type,
t.  device_brand                      t_device_brand,
t.  device_model                      t_device_model,

t.  age                               t_age,
t.  gender                            t_gender,

t.  cost_currency                     t_cost_currency,

-- demand
t.  demand_bd_id                      t_demand_bd_id,
t.  demand_am_id                      t_demand_am_id,
t.  demand_id                         t_demand_id,

-- destination
t.  demand_seat_id                    t_demand_seat_id,
t.  demand_campaign_id                t_demand_campaign_id,
t.  demand_protocol                   t_demand_protocol,
t.  target_site_app_id                t_target_site_app_id,
t.  revenue_currency                  t_revenue_currency,

-- common
t.  bid_price_model                   t_bid_price_model,
t.  traffic_type                      t_traffic_type,
t.  currency                          t_currency,

-- id
t.  supplyBidId                       t_supplyBidId,
t.  bidRequestId                      t_bidRequestId,

t.  bundle                            t_bundle,
t.  size                              t_size,

t.  supply_request_count              t_supply_request_count,
t.  supply_invalid_request_count      t_supply_invalid_request_count,
t.  supply_bid_count                  t_supply_bid_count,
t.  supply_bid_price_cost_currency    t_supply_bid_price_cost_currency,
t.  supply_bid_price                  t_supply_bid_price,
t.  supply_win_count                  t_supply_win_count,
t.  supply_win_price_cost_currency    t_supply_win_price_cost_currency,
t.  supply_win_price                  t_supply_win_price,

t.  demand_request_count              t_demand_request_count,
t.  demand_bid_count                  t_demand_bid_count,
t.  demand_bid_price_revenue_currency t_demand_bid_price_revenue_currency,
t.  demand_bid_price                  t_demand_bid_price,
t.  demand_win_count                  t_demand_win_count,
t.  demand_win_price_revenue_currency t_demand_win_price_revenue_currency,
t.  demand_win_price                  t_demand_win_price,
t.  demand_timeout_count              t_demand_timeout_count,

t.  impression_count                  t_impression_count,
t.  impression_cost_currency          t_impression_cost_currency,
t.  impression_cost                   t_impression_cost,
t.  impression_revenue_currency       t_impression_revenue_currency,
t.  impression_revenue                t_impression_revenue,
t.  click_count                       t_click_count,
t.  click_cost_currency               t_click_cost_currency,
t.  click_cost                        t_click_cost,
t.  click_revenue_currency            t_click_revenue_currency,
t.  click_revenue                     t_click_revenue,
t.  conversion_count                  t_conversion_count,
t.  conversion_price                  t_conversion_price,
t.  saveCount                         t_saveCount,
t. b_time

from (
  select * from(
    select
      row_number() over(partition by `type`, bidid order by 1 desc) row_num,
      *
    from nadx_performance_dwi
    where b_time = '2019-03-20 03:00:00'
  )t0
  where row_num = 1
--   limit 1
) p
left join (
--   select * from nadx_traffic_dwi where b_time = '2019-03-20 03:00:00' and dataType = 4

  select * from(
    select
      row_number() over(partition by datatype, bidRequestId order by 1 desc) row_num2,
      *
    from nadx_traffic_dwi where b_time = '2019-03-20 03:00:00' and dataType = 4
  )t0
  where row_num2 = 1
) t on p.bidid = t.bidRequestId and p.b_time = t.b_time
-- where t.demand_id = 5

------03:00 =2279
select count(1) from nadx_joined_performance_dwi_dataType4_qu_chong
where t_demand_id = 5  and b_time = '2019-03-20 04:00:00' and p_type ='impression'

select p_type, count(1) from nadx_joined_performance_dwi_dataType4_qu_chong
where t_demand_id = 5  and b_time = '2019-03-20 04:00:00'
group by p_type
+-------------+-----------+--+
|   p_type    | count(1)  |
+-------------+-----------+--+
| impression  | 2565      |
| win         | 2528      |
| click       | 7         |


----------------------------------
select p_bidid, p_type,count(1) from nadx_joined_performance_dwi_dataType4_qu_chong
where t_demand_id = 5  and b_time = '2019-03-20 04:00:00'
group by p_bidid,p_type
order by 3 desc
limit 1

----
select p_bidid, p_type,count(1) from nadx_joined_performance_dwi_dataType4
where t_demand_id = 5  and b_time = '2019-03-20 04:00:00'
group by p_bidid,p_type
order by 3 desc
limit 1

-----------------------
select repeated,count(1) from nadx_performance_dwi where b_time = '2019-03-21 05:00:00'
group by repeated
----------------------------

select count(1)
from(
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 05:00:00'
) n
left join (
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 05:00:00'
) y on n.bidid = y.bidid and n.type = y.type
where y.bidid is null

---------------
-- 没有重复的
select n.*, y.*
from(
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 05:00:00'
) n
left join (
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 05:00:00'
) y on n.bidid = y.bidid and n.type = y.type
where y.bidid is null
limit 1

---------------
-- 误判为重复数据的
select y.b_time, count(1)
from(
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time >= '2019-03-21 00:00:00'
) y
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time >= '2019-03-21 00:00:00'
) n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null
group by y.b_time
order by 1

----------- = 17
-- 误划分为 重复数据的，到了Y分区
select y.b_time, count(1)
from(
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 04:00:00'
) y
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 04:00:00'
) n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null
group by y.b_time
order by 1

-- 误划分为 重复数据的，到了Y分区的明细
select y.*
from(
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 04:00:00'
) y
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 04:00:00'
) n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null

-----

select count(1) from nadx_performance_dwi where  b_time = '2019-03-21 12:00:00'

-- 对误划分为 重复数据的， 去重后的数据量
select count(distinct  rowkey) from (
select y.*
from(
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 04:00:00'
) y
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 04:00:00'
) n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null)

----------

-- = 24093
select count(1) from nadx_performance_dwi where b_time = '2019-03-21 04:00:00'
select repeated,count(1) from nadx_performance_dwi where b_time = '2019-03-21 04:00:00' group by repeated
select repeated,count(distinct(type, bidid)) from nadx_performance_dwi where b_time = '2019-03-21 04:00:00' group by repeated
-- | repeated  | count(DISTINCT named_struct(type, type, bidid, bidid))  |
-- +-----------+---------------------------------------------------------+--+
-- | Y         | 2435                                                    |
-- | N         | 19786                                                   |
-- +-----------+---------------------------------------------------------+--+



-- = 19800
select count(distinct(type, bidid)) from nadx_performance_dwi where b_time = '2019-03-21 04:00:00'

-- = 19786  少了14
select count(1) from nadx_performance_dwi where b_time = '2019-03-21 04:00:00' and  repeated = 'N'

--------------

select count(1) from(
  select y.*,n.*
  from(
    select * from nadx_performance_dwi where repeated = 'Y' AND b_time = '2019-03-21 04:00:00'
  ) y
  left join (
    select * from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 04:00:00'
  ) n on n.bidid = y.bidid and n.type = y.type
  where n.bidid is null
)y
left join (
  select * from nadx_performance_dwi where b_time = '2019-03-21 04:00:00' and  repeated = 'N'
)n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null

------

select count(distinct bidid) from nadx_performance_dwi where repeated = 'N' AND b_time = '2019-03-21 00:00:00'

-----------

select * from nadx_performance_dwi
where    b_time = '2019-03-21 05:00:00' and
rowkey = 'impression^200030D7F8409DF1F684E41553118131838'

-----------------------

select repeated, count(distinct (dataType, bidRequestId))
from nadx_traffic_dwi where b_time = '2019-03-21 14:00:00'
group by repeated

---
select count(distinct (dataType, bidRequestId)) from nadx_traffic_dwi where b_time = '2019-03-21 04:00:00'
-- | repeated  | count(DISTINCT named_struct(dataType, dataType, bidRequestId, bidRequestId))  |
-- +-----------+-------------------------------------------------------------------------------+--+
-- | Y         | 4352324                                                                       |
-- | N         | 11744826                                                                      |
-- +-----------+-------------------------------------------------------------------------------+
select count(1) from nadx_traffic_dwi where b_time = '2019-03-21 04:00:00'

---

select demand_id,repeated, count(distinct(dataType,bidRequestId))
from nadx_traffic_dwi where b_time = '2019-03-21 13:00:00'
group by demand_id,repeated

--------------

select * from nadx_traffic_dwi where repeated = 'Y' and b_time = '2019-03-21 13:00:00' limit 1


select * from nadx_traffic_dwi where repeated = 'N' and b_time = '2019-03-21 13:00:00' AND rowkey ='2^2000D43375EBC9F1F543861553144978155'


----
-- 误划分为 重复数据的，到了Y分区
select y.b_time, count(1)
from(
  select * from nadx_traffic_dwi where repeated = 'Y' AND b_time >= '2019-03-21 14:00:00'
) y
left join (
  select * from nadx_traffic_dwi where repeated = 'N' AND b_time >= '2019-03-21 14:00:00'
) n on n.bidRequestId = y.bidRequestId and n.dataType = y.dataType
where n.bidRequestId is null
group by y.b_time
order by 1




---------------

select
    dataType,
    sum(supply_request_count) as supply_request_count,
    sum(supply_invalid_request_count) as supply_invalid_request_count,
    sum(supply_bid_count) as supply_bid_count,
    sum(supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
    sum(supply_bid_price) as supply_bid_price,
    sum(supply_win_price_cost_currency) as supply_win_price_cost_currency,
    sum(supply_win_price) as supply_win_price,
    sum(demand_request_count) as demand_request_count,
    sum(demand_bid_count) as demand_bid_count,
    sum(demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
    sum(demand_bid_price) as demand_bid_price,
    sum(demand_win_count) as demand_win_count,
    sum(demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
    sum(demand_win_price) as demand_win_price,
    sum(demand_timeout_count) as demand_timeout_count,
    sum(impression_count) as impression_count,
    sum(impression_cost_currency) as impression_cost_currency,
    sum(impression_cost) as impression_cost,
    sum(impression_revenue_currency) as impression_revenue_currency,
    sum(impression_revenue) as impression_revenue,
    sum(click_count) as click_count,
    sum(click_cost_currency) as click_cost_currency,
    sum(click_cost) as click_cost,
    sum(click_revenue_currency) as click_revenue_currency,
    sum(click_revenue) as click_revenue,
    sum(conversion_count) as conversion_count,
    sum(conversion_price) as conversion_price
from nadx_traffic_dwi
where
-- repeated = 'N' AND
supply_id = 4 and b_time = '2019-03-20 03:00:00'
group by dataType

-----------------------------
-- ex:     3-19 19:00
-- advent: 3-19 12:00
-- bi:     3-20 03:00
------------------------
select
    p_type,
    sum(t_supply_request_count) as supply_request_count,
    sum(t_supply_invalid_request_count) as supply_invalid_request_count,
    sum(t_supply_bid_count) as supply_bid_count,
    sum(t_supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
    sum(t_supply_bid_price) as supply_bid_price,
    sum(t_supply_win_price_cost_currency) as supply_win_price_cost_currency,
    sum(t_supply_win_price) as supply_win_price,
    sum(t_demand_request_count) as demand_request_count,
    sum(t_demand_bid_count) as demand_bid_count,
    sum(t_demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
    sum(t_demand_bid_price) as demand_bid_price,
    sum(t_demand_win_count) as demand_win_count,
    sum(t_demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
    sum(t_demand_win_price) as demand_win_price,
    sum(t_demand_timeout_count) as demand_timeout_count,
    sum(t_impression_count) as impression_count,
    sum(t_impression_cost_currency) as impression_cost_currency,
    sum(t_impression_cost) as impression_cost,
    sum(t_impression_revenue_currency) as impression_revenue_currency,
    sum(t_impression_revenue) as impression_revenue,
    sum(t_click_count) as click_count,
    sum(t_click_cost_currency) as click_cost_currency,
    sum(t_click_cost) as click_cost,
    sum(t_click_revenue_currency) as click_revenue_currency,
    sum(t_click_revenue) as click_revenue,
    sum(t_conversion_count) as conversion_count,
    sum(t_conversion_price) as conversion_price
from nadx_joined_performance_dwi_dataType4_qu_chong
where
-- repeated = 'N' AND
t_supply_id = 4
 and b_time = '2019-03-20 03:00:00'
group by p_type

-------------

select
    p_type,
    sum(t_supply_request_count) as supply_request_count,
    sum(t_supply_invalid_request_count) as supply_invalid_request_count,
    sum(t_supply_bid_count) as supply_bid_count,
    sum(t_supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
    sum(t_supply_bid_price) as supply_bid_price,
    sum(t_supply_win_price_cost_currency) as supply_win_price_cost_currency,
    sum(t_supply_win_price) as supply_win_price,
    sum(t_demand_request_count) as demand_request_count,
    sum(t_demand_bid_count) as demand_bid_count,
    sum(t_demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
    sum(t_demand_bid_price) as demand_bid_price,
    sum(t_demand_win_count) as demand_win_count,
    sum(t_demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
    sum(t_demand_win_price) as demand_win_price,
    sum(t_demand_timeout_count) as demand_timeout_count,
    sum(t_impression_count) as impression_count,
    sum(t_impression_cost_currency) as impression_cost_currency,
    sum(t_impression_cost) as impression_cost,
    sum(t_impression_revenue_currency) as impression_revenue_currency,
    sum(t_impression_revenue) as impression_revenue,
    sum(t_click_count) as click_count,
    sum(t_click_cost_currency) as click_cost_currency,
    sum(t_click_cost) as click_cost,
    sum(t_click_revenue_currency) as click_revenue_currency,
    sum(t_click_revenue) as click_revenue,
    sum(t_conversion_count) as conversion_count,
    sum(t_conversion_price) as conversion_price
from nadx_joined_performance_dwi_dataType4_qu_chong
where
-- repeated = 'N' AND
t_supply_id = 7
 and b_time = '2019-03-20 03:00:00'
group by p_type

-------------------
select
    dataType,
    sum(supply_request_count) as supply_request_count,
    sum(supply_invalid_request_count) as supply_invalid_request_count,
    sum(supply_bid_count) as supply_bid_count,
    sum(supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
    sum(supply_bid_price) as supply_bid_price,
    sum(supply_win_price_cost_currency) as supply_win_price_cost_currency,
    sum(supply_win_price) as supply_win_price,
    sum(demand_request_count) as demand_request_count,
    sum(demand_bid_count) as demand_bid_count,
    sum(demand_bid_price_revenue_currency) as demand_bid_price_revenue_currency,
    sum(demand_bid_price) as demand_bid_price,
    sum(demand_win_count) as demand_win_count,
    sum(demand_win_price_revenue_currency) as demand_win_price_revenue_currency,
    sum(demand_win_price) as demand_win_price,
    sum(demand_timeout_count) as demand_timeout_count,
    sum(impression_count) as impression_count,
    sum(impression_cost_currency) as impression_cost_currency,
    sum(impression_cost) as impression_cost,
    sum(impression_revenue_currency) as impression_revenue_currency,
    sum(impression_revenue) as impression_revenue,
    sum(click_count) as click_count,
    sum(click_cost_currency) as click_cost_currency,
    sum(click_cost) as click_cost,
    sum(click_revenue_currency) as click_revenue_currency,
    sum(click_revenue) as click_revenue,
    sum(conversion_count) as conversion_count,
    sum(conversion_price) as conversion_price
from nadx_traffic_dwi
where
-- repeated = 'N' AND
supply_id = 7 and b_time = '2019-03-20 03:00:00'
group by dataType
----------------------------




-- 误划分为 重复数据的，到了Y分区
select y.b_time, count(1)
from(
  select * from nadx_performance_dwi where repeated = 'Y' AND b_time >= '2019-03-22 15:00:00'
) y
left join (
  select * from nadx_performance_dwi where repeated = 'N' AND b_time >= '2019-03-22 15:00:00'
) n on n.bidid = y.bidid and n.type = y.type
where n.bidid is null
group by y.b_time
order by 1

--------------
-- 28774874
SELECT COUNT(distinct( concat_ws("^", dataType, bidRequestId, if(demand_id is null, '', demand_id) ) ) )
FROM nadx_traffic_dwi
where b_time = '2019-03-22 15:00:00'

-- 28775277
SELECT COUNT(1)
FROM nadx_traffic_dwi
where b_time = '2019-03-22 15:00:00'

-- 28774874
SELECT COUNT(distinct(dataType, bidRequestId, if(demand_id is null, '', demand_id)))
FROM nadx_traffic_dwi
where b_time = '2019-03-22 15:00:00';


select count(1) from nadx_performance_dwi where repeated = 'N' AND b_time >= '2019-03-22 15:00:00'

select count(1) from nadx_performance_dwi where repeated = 'Y' AND b_time >= '2019-03-22 15:00:00'

----------------------

select * from nadx_traffic_dwi where supply_id = 40 and b_time >= '2019-03-22 15:00:00' limit 1

