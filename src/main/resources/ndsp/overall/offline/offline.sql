-----------------------------------------------------------------------------------------
-- 离线统计
-----------------------------------------------------------------------------------------
-- spark-sql --driver-memory  8G   --executor-memory 12G
-- spark-sql --master yarn --hivevar start_b_time="`date "+%Y-%m-%d 00:00:00" -d "-1 days"`" --hivevar end_b_time="`date "+%Y-%m-%d 23:00:00" -d "-1 days"`" --hivevar b_date="`date "+%Y-%m-%d" -d "-1 days"`" -f ~/offline.crontab.sql  > offline.log 2>&1

-- beeline -n "" -p ""  -u jdbc:hive2://master:10016/default --outputformat=table --verbose=true

-- 强烈建议用Orc格式，避免字段值含分隔符导致数据错位问题！
set hive.default.fileformat=Orc;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.default.parallelism = 4;
set spark.sql.shuffle.partitions = 4;
set start_b_date = "${start_b_date}";
set start_b_time = "${start_b_time}";

set log = "Overwrite ndsp_overall_day_dwr START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";

insert overwrite table ndsp_overall_day_dwr partition(b_date=${start_b_date},b_time=${start_b_time},l_time=${start_b_time},b_version=0)
select distinct concat_ws('-',concat(${start_b_date},mediabuy_id),concat('',campaign_id),concat('',ad_id)) as id, mediabuy_id,campaign_id,ad_id,sum(impression_count) as impression_count,
sum(click_count) as click_count,
cast(sum(impression_cost) as decimal(10,4)) as impression_cost,
cast(sum(impression_revenue) as decimal(10,4)) as impression_revenue
 from ndsp_overall_dwr where  b_date=${start_b_date}  group by mediabuy_id,campaign_id,ad_id;

set log = "Overwrite ndsp_overall_day_dwr DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
