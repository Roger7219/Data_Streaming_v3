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
  saveCount                         int
  
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

CREATE TABLE nadx_overall_performance_matched_dwi(
  repeats                           int,
  rowkey                            string,

  `type`        string,
  bidTime       bigint, -- unix timestamp in second
  supplyid      int,
  bidid         string,
  impid         string,
  price         double,
  cur           string,
  withPrice     boolean,
  eventType     int,

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




