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

--
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert into table  ssp_log_dwr_tmp
-- select
-- int(appid) appid,
-- event,
-- times,successtimes, l_time,b_date , cast(concat(b_date, ' 00:00:00') as string) as b_time
-- from ssp_log_dwr
-- where b_date>='2019-01-01' and b_date<'2020-01-01'
--
--
--
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set spark.default.parallelism = 1;
-- set spark.sql.shuffle.partitions = 1;
-- insert into table  ssp_log_dwr_tmp
-- select
-- int(appid) appid,
-- event,
-- times,successtimes, l_time,b_date , cast(concat(b_date, ' 00:00:00') as string) as b_time
-- from ssp_log_dwr
-- where b_date>='2020-01-01'
-- --
--
-- create table  ssp_log_dwr_tmp2
-- select
-- int(appid) as appid,
-- event,
-- times,
-- successtimes,
-- l_time,
-- b_date ,
-- b_time
-- from create table  ssp_log_dwr_tmp

