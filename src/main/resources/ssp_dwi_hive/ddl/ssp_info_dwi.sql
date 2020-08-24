CREATE TABLE `ssp_info_dwi`(
  `repeats` int,
  `rowkey` string,
  `appid` int,
  `imei` string,
  `packagename` string,
  `createtime` string)
PARTITIONED BY (
  `repeated` string,
  `l_time` string,
  `b_date` string,
  `b_time` string)
STORED AS ORC;


-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert into ssp_info_dwi_tmp
--  select
--   *,
--   from_unixtime(unix_timestamp(createTime), 'yyyy-MM-dd HH:00:00')
--  from ssp_info_dwi;
