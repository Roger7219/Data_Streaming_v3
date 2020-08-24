CREATE TABLE `ssp_log_dwi`(
  `repeats` int,
  `rowkey` string,
  `appid` int,
  `createtime` string,
  `event` string,
  `imei` string,
  `imsi` string,
  `info` string,
  `type` int)
PARTITIONED BY (
  `repeated` string,
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;
