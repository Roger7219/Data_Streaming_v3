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
