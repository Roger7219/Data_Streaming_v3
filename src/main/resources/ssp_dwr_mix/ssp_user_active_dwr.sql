CREATE TABLE `ssp_user_active_dwr`(
  `appid` int,
  `countryid` int,
  `carrierid` int,
  `sv` string,
  `affsub` string,
  `activecount` bigint,
  `operatingsystem` string,
  `systemlanguage` string,
  `devicebrand` string,
  `devicetype` string,
  `browserkernel` string)
PARTITIONED BY (
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;


-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert overwrite table ssp_user_active_dwr_tmp
-- select
--   `appid` ,
--   `countryid` ,
--   `carrierid` ,
--   `sv` ,
--   `affsub` ,
--   `activecount` ,
--   `operatingsystem` ,
--   `systemlanguage` ,
--   `devicebrand` ,
--   `devicetype` ,
--   `browserkernel` ,
--   `l_time` ,
--   `b_date` ,
--   `b_time`
-- from ssp_user_active_dwr
-- where b_date>="2020-08-15" limit 12419274
