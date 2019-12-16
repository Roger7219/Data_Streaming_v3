drop table hivex_overall_dwr;
CREATE TABLE hivex_overall_dwr(
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
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;


drop view if exists hivex_overall_dm;
create view hivex_overall_dm as
select *
from hivex_overall_dwr;
