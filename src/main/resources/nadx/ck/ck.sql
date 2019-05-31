CREATE TABLE nadx_overall_dm (
  supply_bd_id                      Int32 DEFAULT CAST(0 AS Int32),
  supply_am_id                      Int32 DEFAULT CAST(0 AS Int32),
  supply_id                         Int32 DEFAULT CAST(0 AS Int32),
  supply_protocol                   Int32 DEFAULT CAST(0 AS Int32),
  request_flag                      Int32 DEFAULT CAST(0 AS Int32),
  ad_format                         Int32 DEFAULT CAST(0 AS Int32),
  site_app_id                       Int32 DEFAULT CAST(0 AS Int32),
  placement_id                      Int32 DEFAULT CAST(0 AS Int32),
  position                          Int32 DEFAULT CAST(0 AS Int32),
  country                           Nullable(String),
  state                             Nullable(String),
  city                              Nullable(String),
  carrier                           Nullable(String),
  os                                Nullable(String),
  os_version                        Nullable(String),
  device_type                       Int32 DEFAULT CAST(0 AS Int32),
  device_brand                      Nullable(String),
  device_model                      Nullable(String),
  age                               Nullable(String),
  gender                            Nullable(String),
  cost_currency                     Nullable(String),
  demand_bd_id                      Int32 DEFAULT CAST(0 AS Int32),
  demand_am_id                      Int32 DEFAULT CAST(0 AS Int32),
  demand_id                         Int32 DEFAULT CAST(0 AS Int32),
  demand_seat_id                    Nullable(String),
  demand_campaign_id                Nullable(String),
  demand_protocol                   Int32 DEFAULT CAST(0 AS Int32),
  target_site_app_id                Nullable(String),
  revenue_currency                  Nullable(String),
  bid_price_model                   Int32 DEFAULT CAST(0 AS Int32),
  traffic_type                      Int32 DEFAULT CAST(0 AS Int32),
  currency                          Nullable(String),
  bundle                            Nullable(String),
  size                              Nullable(String),

  supply_request_count              Int64 DEFAULT CAST(0 AS Int64),
  supply_invalid_request_count      Int64 DEFAULT CAST(0 AS Int64),
  supply_bid_count                  Int64 DEFAULT CAST(0 AS Int64),
  supply_bid_price_cost_currency    Float64 DEFAULT CAST(0. AS Float64),
  supply_bid_price                  Float64 DEFAULT CAST(0. AS Float64),
  supply_win_count                  Int64 DEFAULT CAST(0 AS Int64),
  supply_win_price_cost_currency    Float64 DEFAULT CAST(0. AS Float64),
  supply_win_price                  Float64 DEFAULT CAST(0. AS Float64),
  demand_request_count              Int64 DEFAULT CAST(0 AS Int64),
  demand_bid_count                  Int64 DEFAULT CAST(0 AS Int64),
  demand_bid_price_revenue_currency Float64 DEFAULT CAST(0. AS Float64),
  demand_bid_price                  Float64 DEFAULT CAST(0. AS Float64),
  demand_win_count                  Int64 DEFAULT CAST(0 AS Int64),
  demand_win_price_revenue_currency Float64 DEFAULT CAST(0. AS Float64),
  demand_win_price                  Float64 DEFAULT CAST(0. AS Float64),
  demand_timeout_count              Int64 DEFAULT CAST(0 AS Int64),
  impression_count                  Int64 DEFAULT CAST(0 AS Int64),
  impression_cost_currency          Float64 DEFAULT CAST(0. AS Float64),
  impression_cost                   Float64 DEFAULT CAST(0. AS Float64),
  impression_revenue_currency       Float64 DEFAULT CAST(0. AS Float64),
  impression_revenue                Float64 DEFAULT CAST(0. AS Float64),
  click_count                       Int64 DEFAULT CAST(0 AS Int64),
  click_cost_currency               Float64 DEFAULT CAST(0. AS Float64),
  click_cost                        Float64 DEFAULT CAST(0. AS Float64),
  click_revenue_currency            Float64 DEFAULT CAST(0. AS Float64),
  click_revenue                     Float64 DEFAULT CAST(0. AS Float64),
  conversion_count                  Int64 DEFAULT CAST(0 AS Int64),
  conversion_price                  Float64 DEFAULT CAST(0. AS Float64),
  l_time                            DateTime,
  b_date                            Date,
  b_time                            DateTime,
  b_version                         Nullable(String)
)
ENGINE = MergeTree PARTITION BY (b_date, b_time) ORDER BY (b_date, b_time) SETTINGS index_granularity = 8192;

CREATE TABLE nadx_overall_dm_all AS nadx_overall_dm ENGINE = Distributed(bip_ck_cluster, default, nadx_overall_dm, rand());

CREATE TABLE nadx_overall_dm_for_select AS nadx_overall_dm;
CREATE TABLE nadx_overall_dm_for_select_all AS nadx_overall_dm_for_select ENGINE = Distributed(bip_ck_cluster, default, nadx_overall_dm_for_select, rand());

------2019-04-19 新增字段
ALTER TABLE nadx_overall_dm add column tips Nullable(String);
ALTER TABLE nadx_overall_dm add column node Nullable(String);
ALTER TABLE nadx_overall_dm_all add column tips Nullable(String);
ALTER TABLE nadx_overall_dm_all add column node Nullable(String);
ALTER TABLE nadx_overall_dm_for_select add column tips Nullable(String);
ALTER TABLE nadx_overall_dm_for_select add column node Nullable(String);
ALTER TABLE nadx_overall_dm_for_select_all add column tips Nullable(String);
ALTER TABLE nadx_overall_dm_for_select_all add column node Nullable(String);

-- 2019-04-23 新增字段，取代tips
ALTER TABLE nadx_overall_dm add column tip_type Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_all add column tip_type Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select add column tip_type Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select_all add column tip_type Int32 DEFAULT CAST(0 AS Int32);


-- 2019-05-03 新增字段
ALTER TABLE nadx_overall_dm add column tip_desc Nullable(String);
ALTER TABLE nadx_overall_dm_all add column tip_desc Nullable(String);
ALTER TABLE nadx_overall_dm_for_select add column tip_desc Nullable(String);
ALTER TABLE nadx_overall_dm_for_select_all add column tip_desc Nullable(String);
ALTER TABLE nadx_overall_dm add column event_count Int64 DEFAULT CAST(0 AS Int64);
ALTER TABLE nadx_overall_dm_all add column event_count Int64 DEFAULT CAST(0 AS Int64);
ALTER TABLE nadx_overall_dm_for_select add column event_count Int64 DEFAULT CAST(0 AS Int64);
ALTER TABLE nadx_overall_dm_for_select_all add column event_count Int64 DEFAULT CAST(0 AS Int64);
ALTER TABLE nadx_overall_dm add column ssp_token Nullable(String);
ALTER TABLE nadx_overall_dm_all add column ssp_token Nullable(String);
ALTER TABLE nadx_overall_dm_for_select add column ssp_token Nullable(String);
ALTER TABLE nadx_overall_dm_for_select_all add column ssp_token Nullable(String);
-- 2019-06-01 新增字段
ALTER TABLE nadx_overall_dm add column rtb_version Nullable(String);
ALTER TABLE nadx_overall_dm_all add column rtb_version Nullable(String);
ALTER TABLE nadx_overall_dm_for_select add column rtb_version Nullable(String);
ALTER TABLE nadx_overall_dm_for_select_all add column rtb_version Nullable(String);
ALTER TABLE nadx_overall_dm add column demand_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_all add column demand_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select add column demand_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select_all add column demand_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm add column adx_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_all add column adx_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select add column adx_using_time Int32 DEFAULT CAST(0 AS Int32);
ALTER TABLE nadx_overall_dm_for_select_all add column adx_using_time Int32 DEFAULT CAST(0 AS Int32);



------2019-04-20 新增审核送检数据表
CREATE TABLE nadx_overall_audit_dm (
  demand_id                         Int32 DEFAULT CAST(0 AS Int32),
  crid                              Nullable(String),
  os                                Nullable(String),
  country                           Nullable(String),
  adm                               Nullable(String),
  demand_crid_count                 Int64 DEFAULT CAST(0 AS Int64),

  l_time                            DateTime,
  b_date                            Date,
  b_time                            DateTime,
  b_version                         Nullable(String)
)
ENGINE = MergeTree PARTITION BY (b_date, b_time) ORDER BY (b_date, b_time) SETTINGS index_granularity = 8192;
CREATE TABLE nadx_overall_audit_dm_all AS nadx_overall_audit_dm ENGINE = Distributed(bip_ck_cluster, default, nadx_overall_audit_dm, rand());

CREATE TABLE nadx_overall_audit_dm_for_select AS nadx_overall_audit_dm;
CREATE TABLE nadx_overall_audit_dm_for_select_all AS nadx_overall_audit_dm_for_select ENGINE = Distributed(bip_ck_cluster, default, nadx_overall_audit_dm_for_select, rand());
