CREATE TABLE hivex_overall_dm (
  app_key                           Nullable(String),
  order_key                         Nullable(String),
  adunit_key                        Nullable(String),
  lineitem_key                      Nullable(String),
  appid                             Nullable(String),
  cid                               Nullable(String),
  city                              Nullable(String),
  ckv                               Nullable(String),
  country_code                      Nullable(String),
  cppck                             Nullable(String),
  current_consent_status            Nullable(String),
  dev                               Nullable(String),
  exclude_adgroups                  Nullable(String),
  gdpr_applies                      Nullable(String),
  id                                Nullable(String),
  is_mraid                          Nullable(String),
  os                                Nullable(String),
  osv                               Nullable(String),
  priority                          Nullable(String),
  req                               Nullable(String),
  reqt                              Nullable(String),
  rev                               Nullable(String),
  udid                              Nullable(String),
  video_type                        Nullable(String),
  request_count                     Int64 DEFAULT CAST(0 AS Int64),
  imp_count                         Int64 DEFAULT CAST(0 AS Int64),
  aclk_count                        Int64 DEFAULT CAST(0 AS Int64),
  attempt_count                     Int64 DEFAULT CAST(0 AS Int64),
  l_time                            DateTime,
  b_date                            Date,
  b_time                            DateTime,
  b_version                         Nullable(String)
)
ENGINE = MergeTree PARTITION BY (b_date, b_time) ORDER BY (b_date, b_time) SETTINGS index_granularity = 8192;
CREATE TABLE hivex_overall_dm_all AS hivex_overall_dm ENGINE = Distributed(bip_ck_cluster, default, hivex_overall_dm, rand());
CREATE TABLE hivex_overall_dm_for_select AS hivex_overall_dm;
CREATE TABLE hivex_overall_dm_for_select_all AS hivex_overall_dm_for_select ENGINE = Distributed(bip_ck_cluster, default, hivex_overall_dm_for_select, rand());