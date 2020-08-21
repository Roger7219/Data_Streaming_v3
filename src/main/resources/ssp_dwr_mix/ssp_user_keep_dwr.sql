CREATE TABLE `ssp_user_keep_dwr`(
  `appid` int,
  `countryid` int,
  `carrierid` int,
  `sv` string,
  `affsub` string,
  `activedate` string,
  `usercount` bigint,
  `firstcount` bigint,
  `secondcount` bigint,
  `thirdcount` bigint,
  `fourthcount` bigint,
  `fifthcount` bigint,
  `sixthcount` bigint,
  `seventhcount` bigint,
  `fiftycount` bigint,
  `thirtycount` bigint,
  `operatingsystem` string,
  `systemlanguage` string,
  `devicebrand` string,
  `devicetype` string,
  `browserkernel` string
  )
PARTITIONED BY (
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;

-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert overwrite table ssp_user_keep_dwr_tmp
-- select `appid` ,
--   `countryid` ,
--   `carrierid` ,
--   `sv` ,
--   `affsub` ,
--   `activedate` ,
--   `usercount` ,
--   `firstcount` ,
--   `secondcount` ,
--   `thirdcount` ,
--   `fourthcount` ,
--   `fifthcount` ,
--   `sixthcount` ,
--   `seventhcount` ,
--   `fiftycount` ,
--   `thirtycount` ,
--   `operatingsystem` ,
--   `systemlanguage` ,
--   `devicebrand` ,
--   `devicetype` ,
--   `browserkernel`,
--   `l_time` string,
--   `b_date` string,
--   `b_time` string
-- from ssp_user_keep_dwr
-- where b_date>="2020-08-15" limit 114376977
