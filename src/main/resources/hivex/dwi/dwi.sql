drop table hivex_traffic_dwi;
CREATE TABLE hivex_traffic_dwi(
  `timestamp`                           bigint,
  app_key                           string,
  order_key                         string,
  adunit_key                        string,
  lineitem_key                      string,
  appid                             string,
  cid                               string,
  city                              string,
  ckv                               string,
  country_code                      string,
  cppck                             string,
  current_consent_status            string,
  dev                               string,
  exclude_adgroups                  string,
  gdpr_applies                      string,
  id                                string,
  is_mraid                          string,
  os                                string,
  osv                               string,
  priority                          string,
  req                               string,
  reqt                              string,
  rev                               string,
  udid                              string,
  video_type                        string,
  request_count              bigint,
  imp_count                  bigint,
  aclk_count                 bigint,
  attempt_count              bigint
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;
