/*
   ClickHouse目前使用的版本为1.1.54378,在目前的使用版本中没有Decimal的支持，只能通过Float64（与Double相同）进行统计，因此无法做到精确统计
   在18.12.13版本中添加了对Decimal的支持，后续如果要升级版本尽可能升级到18.12.13以后的版本
   官方更新记录地址：https://github.com/yandex/ClickHouse/blob/master/CHANGELOG.md
*/


-- 小时数据上传表
CREATE TABLE test__ssp_report_overall_dm (
  publisherid Int32 DEFAULT CAST(0 AS Int32),
  appid Int32 DEFAULT CAST(0 AS Int32),
  countryid Int32 DEFAULT CAST(0 AS Int32),
  carrierid Int32 DEFAULT CAST(0 AS Int32),
  adtype Int32 DEFAULT CAST(0 AS Int32),
  campaignid Int32 DEFAULT CAST(0 AS Int32),
  offerid Int32 DEFAULT CAST(0 AS Int32),
  imageid Int32 DEFAULT CAST(0 AS Int32),
  affsub Nullable(String),
  requestcount Int64 DEFAULT CAST(0 AS Int64),
  sendcount Int64 DEFAULT CAST(0 AS Int64),
  showcount Int64 DEFAULT CAST(0 AS Int64),
  clickcount Int64 DEFAULT CAST(0 AS Int64),
  feereportcount Int64 DEFAULT CAST(0 AS Int64),
  feesendcount Int64 DEFAULT CAST(0 AS Int64),
  feereportprice Float32 DEFAULT CAST(0. AS Float32),
  feesendprice Float32 DEFAULT CAST(0. AS Float32),
  cpcbidprice Float32 DEFAULT CAST(0. AS Float32),
  cpmbidprice Float32 DEFAULT CAST(0. AS Float32),
  conversion Int64 DEFAULT CAST(0 AS Int64),
  allconversion Int64 DEFAULT CAST(0 AS Int64),
  revenue Float32 DEFAULT CAST(0. AS Float32),
  realrevenue Float32 DEFAULT CAST(0. AS Float32),
  l_time DateTime,
  b_date Date,
  publisheramid Int32 DEFAULT CAST(0 AS Int32),
  publisheramname Nullable(String),
  advertiseramid Int32 DEFAULT CAST(0 AS Int32),
  advertiseramname Nullable(String),
  appmodeid Int32 DEFAULT CAST(0 AS Int32),
  appmodename Nullable(String),
  adcategory1id Int32 DEFAULT CAST(0 AS Int32),
  adcategory1name Nullable(String),
  campaignname Nullable(String),
  adverid Int32 DEFAULT CAST(0 AS Int32),
  advername Nullable(String),
  offeroptstatus Int32 DEFAULT CAST(0 AS Int32),
  offername Nullable(String),
  publishername Nullable(String),
  appname Nullable(String),
  iab1name Nullable(String),
  iab2name Nullable(String),
  countryname Nullable(String),
  carriername Nullable(String),
  adtypeid Int32 DEFAULT CAST(0 AS Int32),
  adtypename Nullable(String),
  versionid Int32 DEFAULT CAST(0 AS Int32),
  versionname Nullable(String),
  publisherproxyid Int32 DEFAULT CAST(0 AS Int32),
  data_type Nullable(String),
  feecpctimes Int64 DEFAULT CAST(0 AS Int64),
  feecpmtimes Int64 DEFAULT CAST(0 AS Int64),
  feecpatimes Int64 DEFAULT CAST(0 AS Int64),
  feecpasendtimes Int64 DEFAULT CAST(0 AS Int64),
  feecpcreportprice Float32 DEFAULT CAST(0. AS Float32),
  feecpmreportprice Float32 DEFAULT CAST(0. AS Float32),
  feecpareportprice Float32 DEFAULT CAST(0. AS Float32),
  feecpcsendprice Float32 DEFAULT CAST(0. AS Float32),
  feecpmsendprice Float32 DEFAULT CAST(0. AS Float32),
  feecpasendprice Float32 DEFAULT CAST(0. AS Float32),
  countrycode Nullable(String),
  packagename Nullable(String),
  domain Nullable(String),
  operatingsystem Nullable(String),
  systemlanguage Nullable(String),
  devicebrand Nullable(String),
  devicetype Nullable(String),
  browserkernel Nullable(String),
  b_time DateTime,
  respstatus Int32 DEFAULT CAST(0 AS Int32),
  winprice Float64 DEFAULT 0.,
  winnotices Int64 DEFAULT CAST(0 AS Int64),
  issecondhighpricewin Int32 DEFAULT CAST(0 AS Int32),
  companyid Int32 DEFAULT CAST(0 AS Int32),
  companyname Nullable(String),
  test Int32 DEFAULT CAST(0 AS Int32),
  ruleid Int32 DEFAULT CAST(0 AS Int32),
  smartid Int32 DEFAULT CAST(0 AS Int32),
  proxyid Int32 DEFAULT CAST(0 AS Int32),
  smartname Nullable(String),
  rulename Nullable(String),
  appcompanyid Int32 DEFAULT CAST(0 AS Int32),
  offercompanyid Int32 DEFAULT CAST(0 AS Int32),
  newcount Int64 DEFAULT CAST(0 AS Int64),
  activecount Int64 DEFAULT CAST(0 AS Int64),
  adcategory2id Int32 DEFAULT CAST(0 AS Int32),
  adcategory2name Nullable(String),
  publisherampaid Int32 DEFAULT CAST(0 AS Int32),
  publisherampaname Nullable(String),
  advertiseramaaid Int32 DEFAULT CAST(0 AS Int32),
  advertiseramaaname Nullable(String),
  eventname Nullable(String),
  recommender Int32 DEFAULT CAST(0 AS Int32),
  ratertype Int32 DEFAULT CAST(0 AS Int32),
  raterid Nullable(String)
) ENGINE = MergeTree PARTITION BY (l_time, b_date, b_time) ORDER BY (l_time, b_date, b_time) SETTINGS index_granularity = 8192;

-- 小时数据上传总表
CREATE TABLE test__ssp_report_overall_dm_all AS test__ssp_report_overall_dm  ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm, rand());


-- 小时数据查询表
CREATE TABLE test__ssp_report_overall_dm_for_select AS test__ssp_report_overall_dm;
-- 小时数据查询总表
CREATE TABLE test__ssp_report_overall_dm_for_select_all AS test__ssp_report_overall_dm_for_select ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm_for_select, rand());


/*  天表  */
CREATE TABLE test__ssp_report_overall_dm_day AS test__ssp_report_overall_dm;
CREATE TABLE test__ssp_report_overall_dm_day_all AS test__ssp_report_overall_dm_day ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm_day, rand());

CREATE TABLE test__ssp_report_overall_dm_day_for_select AS test__ssp_report_overall_dm_day;
CREATE TABLE test__ssp_report_overall_dm_day_for_select_all AS test__ssp_report_overall_dm_day_for_select ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm_day_for_select, rand());


/*  月表  */
CREATE TABLE test__ssp_report_overall_dm_month AS test__ssp_report_overall_dm_day;
CREATE TABLE test__ssp_report_overall_dm_month_all AS test__ssp_report_overall_dm_month ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm_month, rand());

CREATE TABLE test__ssp_report_overall_dm_month_for_select AS test__ssp_report_overall_dm_month;
CREATE TABLE test__ssp_report_overall_dm_month_for_select_all AS test__ssp_report_overall_dm_month_for_select ENGINE = Distributed(hive_like_ck_cluster, default, test__ssp_report_overall_dm_month_for_select, rand());
