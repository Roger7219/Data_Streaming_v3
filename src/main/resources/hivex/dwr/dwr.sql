drop table hivex_overall_dwr;
CREATE TABLE hivex_overall_dwr(
  app_key                           string,
  order_key                         string,
  adunit_key                        string,
  lineitem_key                      string,
  network_key                       string,
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
  attempt_count              bigint,
  revenue                    double
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;


drop view if exists hivex_overall_dm;
create view hivex_overall_dm as
select *
from hivex_overall_dwr
UNION all
select app_key,order_key,adunit_key,lineitem_key,network_key,appid,cid,city,ckv,country_code,cppck,current_consent_status,dev,exclude_adgroups,gdpr_applies,id,is_mraid,os,osv,priority,req,reqt,rev,udid,video_type,
CAST(request_count as BIGINT) as request_count,
CAST(imp_count as BIGINT) as imp_count,
CAST(aclk_count as BIGINT) as aclk_count,
CAST(attempt_count as BIGINT) as attempt_count,
CAST(revenue as double) as revenue,
concat(b_date, ' 00:00:00')  as l_time,
b_date,
concat(b_date, ' 00:00:00') as b_time,
'0' as b_version
from hivex_other_dwr;


drop table hivex_other_dwr;
CREATE TABLE hivex_other_dwr(
  data_source                       bigint,
  app_key                           string,
  order_key                         string,
  adunit_key                        string,
  lineitem_key                      string,
  network_key                       string,
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
  request_count                     string,
  imp_count                         string,
  aclk_count                        string,
  attempt_count                     string,
  revenue                           string
)
PARTITIONED BY (b_date string)
row format serde
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with
SERDEPROPERTIES
("separatorChar"=",","quotechar"="\"")
STORED AS TEXTFILE;


alter table hivex_other_dwr add if not exists partition(b_date='2020-01-06') location 'hdfs://master:8020/apps/hive/warehouse/hivex_other_dwr/b_date=2020-01-06';
alter table hivex_other_dwr add if not exists partition(b_date='2020-01-07') location 'hdfs://master:8020/apps/hive/warehouse/hivex_other_dwr/b_date=2020-01-07';
ALTER TABLE hivex_other_dwr DROP PARTITION (b_date='2020-01-06');
alter table hivex_other_dwr add if not exists partition(b_date='2020-01-08') location 'hdfs://master:8020/apps/hive/warehouse/hivex_other_dwr/b_date=2020-01-08';
