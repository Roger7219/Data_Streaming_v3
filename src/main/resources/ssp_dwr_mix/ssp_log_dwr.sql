CREATE TABLE `ssp_log_dwr`(
  `appid` int,
  `event` string,
  `times` bigint,
  `successtimes` bigint)
PARTITIONED BY (
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;

