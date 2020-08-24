CREATE TABLE `ssp_user_new_dwr`(
  `appid` int,
  `countryid` int,
  `carrierid` int,
  `sv` string,
  `affsub` string,
  `newcount` bigint,
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





