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
