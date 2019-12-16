CREATE TABLE ndsp_traffic_dwi(
  repeats                           int,
  rowkey                            string,
  dataType                          int,
  `timestamp`                       bigint,
  supply_id                         int,
  supply_protocol                   int,
  request_flag                      int,
  request_status                    string,
  ad_format                         int,
  site_app_id                       int,
  placement_id                      int,
  position                          int,
  country                           string,
  region                            string,
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
  proxy_id                          int,
  mediabuy_id                       int,
  bd_id                             int,
  am_id                             int,
  campaign_id                       int,
  ad_id                             int,
  revenue_currency                  string,
  bid_price_model                   int,
  traffic_type                      int,
  currency                          string,
  supplyBidId                       string,
  bidRequestId                      string,
  bundle                            string,
  size                              string,
  supply_request_count              bigint,
  supply_bid_count                  bigint,
  supply_bid_price_cost_currency    double,
  supply_bid_price                  double,
  supply_win_count                  bigint,
  supply_win_price_cost_currency    double,
  supply_win_price                  double,
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
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

CREATE TABLE ndsp_overall_performance_matched_dwi(
  repeats                           int,
  rowkey                            string,
  dataType                          int,
  `timestamp`                       bigint,
  supply_id                         int,
  supply_protocol                   int,
  request_flag                      int,
  request_status                      string,

  ad_format                         int,
  site_app_id                       int,
  placement_id                      int,
  position                          int,

  country                           string,
  region                            string,
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
  proxy_id                          int,
  mediabuy_id                       int,
  bd_id                             int,
  am_id                             int,
  campaign_id                       int,
  ad_id                             int,
  revenue_currency                  string,
  bid_price_model                   int,
  traffic_type                      int,
  currency                          string,
  supplyBidId                       string,
  bidRequestId                      string,

  bundle                            string,
  size                              string,

  supply_request_count              bigint,
  supply_bid_count                  bigint,
  supply_bid_price_cost_currency    double,
  supply_bid_price                  double,
  supply_win_count                  bigint,
  supply_win_price_cost_currency    double,
  supply_win_price                  double,

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
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;


CREATE TABLE ndsp_performance_dwi(
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

//2019-12-16按照小雷的需求添加字段
ALTER TABLE ndsp_traffic_dwi ADD COLUMNS (adver_bid_price double);
ALTER TABLE ndsp_traffic_dwi ADD COLUMNS (adver_bid_price_cost_currency double);