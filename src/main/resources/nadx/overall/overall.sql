CREATE TABLE nadx_overall_traffic_dwi(
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
  saveCount                         int   ,
  rtb_version                       string,
  demand_using_time                 string,
  adx_using_time                    string
  
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

CREATE TABLE nadx_overall_performance_matched_dwi(
  repeats                           int,
  rowkey                            string,

--   `type`        string,
--   bidTime       bigint, -- unix timestamp in second
--   supplyid      int,
--   bidid         string,
--   impid         string,
--   price         double,
--   cur           string,
--   withPrice     boolean,
--   eventType     int,

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
  saveCount                         int,

  bidfloor                          string,
  site_id                           string,
  site_cat                          string,
  site_domain                       string,
  publisher_id                      string,
  app_id                            string,
  tmax                              string,
  ip                                string,
  crid                              string,
  cid                               string,
  tips                              string,
  node                              string,
  tip_type                          int,
  tip_desc                          string,
  adm                               string,
  event_count                       bigint,
  ssp_token                         string,
  rtb_version                       string,
  demand_using_time                 string,
  adx_using_time                    string
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;


CREATE TABLE nadx_overall_performance_unmatched_dwi(
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

CREATE TABLE nadx_overall_dwr(
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
  demand_bd_id                      int,
  demand_am_id                      int,
  demand_id                         int,
  demand_seat_id                    string,
  demand_campaign_id                string,
  demand_protocol                   int,
  target_site_app_id                string,
  revenue_currency                  string,
  bid_price_model                   int,
  traffic_type                      int,
  currency                          string,
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
  conversion_price                  double

)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;

drop view if exists nadx_overall_dm;
create view nadx_overall_dm as
select * from nadx_overall_dwr;


drop view if exists nadx_overall_dm_v2;
create view nadx_overall_dm_v2 as
select * from nadx_overall_dwr_v2;

drop view if exists nadx_overall_dm_v6;
create view nadx_overall_dm_v6 as
select t.*,d.name as demand_name,s.name as supply_name from nadx_overall_dwr_v6 t
left join tb_demand_account d on t.demand_id=d.id
left join tb_supply_account s on t.supply_id=s.id;


drop view if exists nadx_overall_dm_day;
create view nadx_overall_dm_day as
select
cast(null as int) as supply_bd_id,
cast(null as int) as supply_am_id,
supply_id,
cast(null as int) as supply_protocol,
cast(null as int) as request_flag,
cast(null as int) as ad_format,
cast(null as int) as site_app_id,
cast(null as int) as placement_id,
cast(null as int) as position,
cast(null as string) as country,
cast(null as string) as state,
cast(null as string) as city,
cast(null as string) as carrier,
cast(null as string) as os,
cast(null as string) as os_version,
cast(null as int) as device_type,
cast(null as string) as device_brand,
cast(null as string) as device_model,
cast(null as string) as age,
cast(null as string) as gender,
cast(null as string) as cost_currency,
cast(null as int) as demand_bd_id,
cast(null as int) as demand_am_id,
demand_id,
cast(null as string) as demand_seat_id,
cast(null as string) as demand_campaign_id,
cast(null as int) as demand_protocol,
cast(null as string) as target_site_app_id,
cast(null as string) as revenue_currency,
cast(null as int) as bid_price_model,
cast(null as int) as traffic_type,
cast(null as string) as currency,
cast(null as string) as bundle,
cast(null as string) as size,
sum(supply_request_count) as supply_request_count,
sum(supply_invalid_request_count) as supply_invalid_request_count,
sum(supply_bid_count) as supply_bid_count,
sum(supply_bid_price_cost_currency) as supply_bid_price_cost_currency,
sum(supply_bid_price) as supply_bid_price,
sum(supply_win_count) as supply_win_count,
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
sum(conversion_price) as conversion_price,
cast(null as string) as tips,
cast(null as string) as node,
cast(null as string) as tip_type,
cast(null as string) as tip_desc,
cast(null as int) as event_count,
cast(null as string) as ssp_token,
cast(null as string) as rtb_version,
cast(null as string) as demand_using_time,
cast(null as string) as adx_using_time,
l_time,
b_date,
b_time,
b_version
from nadx_overall_dm_v6 group by
supply_id,
demand_id,
l_time,
b_date,
b_time,
b_version;






