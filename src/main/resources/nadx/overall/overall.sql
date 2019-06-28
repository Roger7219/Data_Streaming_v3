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


drop view if exists nadx_overall_dm_day;
create view nadx_overall_dm_day as
select
supply_bd_id,
supply_am_id,
supply_id,
supply_protocol,
request_flag,
ad_format,
site_app_id,
placement_id,
position,
country,
state,
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
demand_bd_id,
demand_am_id,
demand_id,
demand_seat_id,
demand_campaign_id,
demand_protocol,
target_site_app_id,
revenue_currency,
bid_price_model,
traffic_type,
currency,
bundle,
size,
sum(supply_request_count),
sum(supply_invalid_request_count),
sum(supply_bid_count),
sum(supply_bid_price_cost_currency),
sum(supply_bid_price),
sum(supply_win_count),
sum(supply_win_price_cost_currency),
sum(supply_win_price),
sum(demand_request_count),
sum(demand_bid_count),
sum(demand_bid_price_revenue_currency),
sum(demand_bid_price),
sum(demand_win_count),
sum(demand_win_price_revenue_currency),
sum(demand_win_price),
sum(demand_timeout_count),
sum(impression_count),
sum(impression_cost_currency),
sum(impression_cost),
sum(impression_revenue_currency),
sum(impression_revenue),
sum(click_count),
sum(click_cost_currency),
sum(click_cost),
sum(click_revenue_currency),
sum(click_revenue),
sum(conversion_count),
sum(conversion_price),
b_date as l_time,
b_date,
b_date as b_time,
b_version,
tips,
node,
tip_type,
tip_desc,
event_count,
ssp_token,
rtb_version,
demand_using_time,
adx_using_time
from nadx_overall_dm group by
b_date,
supply_bd_id,
supply_am_id,
supply_id,
supply_protocol,
request_flag,
ad_format,
site_app_id,
placement_id,
position,
country,
state,
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
demand_bd_id,
demand_am_id,
demand_id,
demand_seat_id,
demand_campaign_id,
demand_protocol,
target_site_app_id,
revenue_currency,
bid_price_model,
traffic_type,
currency,
bundle,
size,
b_version,
tips,
node,
tip_type,
tip_desc,
event_count,
ssp_token,
rtb_version,
demand_using_time,
adx_using_time
;






