CREATE TABLE `ssp_user_active_dwi`(
  `repeats` int,
  `rowkey` string,
  `imei` string,
  `imsi` string,
  `createtime` string,
  `activetime` string,
  `appid` int,
  `model` string,
  `version` string,
  `sdkversion` int,
  `installtype` int,
  `leftsize` string,
  `androidid` string,
  `useragent` string,
  `ipaddr` string,
  `screen` string,
  `countryid` int,
  `carrierid` int,
  `sv` string,
  `affsub` string,
  `lat` string,
  `lon` string,
  `mac1` string,
  `mac2` string,
  `ssid` string,
  `lac` int,
  `cellid` int,
  `ctype` int)
PARTITIONED BY (
  `repeated` string,
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;



-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert overwrite table ssp_user_active_dwi_tmp
-- select *,
-- from_unixtime(unix_timestamp(createTime), 'yyyy-MM-dd HH:00:00') from ssp_user_active_dwi
-- where b_date>="2020-08-20"
-- limit 100000000