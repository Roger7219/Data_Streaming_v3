### FOR Handler
create table ssp_campaign_totalcost_dwr(
    campaignId      INT,
    totalCost       decimal(19,10)
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


create table ssp_campaign_daily_dwr(
    campaignId      INT,
    showCount       BIGINT,
    clickCount       BIGINT,
    dailyCost       decimal(19,10)
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;




------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_campaign_totalcost_dwr_view;
create view ssp_campaign_totalcost_dwr_view as
select
    campaignId,
    sum(totalCost) as totalCost,
    l_time,
    b_date
from (
    select
        campaignId,
        0 as totalCost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01') AS b_date
    from ssp_fill_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')

    UNION ALL
    select
        campaignId,
        sum(cpcBidPrice) as totalCost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01')
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')

    UNION ALL
    select
        campaignId as campaignId,
        sum(cpmBidPrice) as totalCost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01')
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')
)t0
group by
    campaignId,
    l_time,
    b_date;

--CREATE TABLE IF NOT EXISTS ssp_campaign_totalcost_dwr_tmp like ssp_campaign_totalcost_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_campaign_totalcost_dwr
select * from ssp_campaign_totalcost_dwr_view;
--where b_date <'2017-09-10';


DROP VIEW if exists ssp_campaign_daily_dwr_view;
CREATE VIEW ssp_campaign_daily_dwr_view as
select
    campaignId,
    sum(showCount)        as showCount,
    sum(clickCount)       as clickCount,
    sum(dailyCost)        as dailyCost,
    l_time,
    b_date
from(
    select
        campaignId as campaignId,
        0  as showCount,
        0 as clickCount,
        sum(reportPrice)  as dailyCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fee_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    SELECT
        campaignId,
        0 as showCount,
        sum(times) as clickCount,
        sum(cpcBidPrice) as dailyCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        campaignId,
        sum(times)  as showCount,
        0           as clickCount,
        sum(cpmBidPrice)  as dailyCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        campaignId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date
)t0
group by
    campaignId,
    l_time,
    b_date;


--CREATE TABLE IF NOT EXISTS ssp_campaign_daily_dwr_tmp like ssp_campaign_daily_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_campaign_daily_dwr
select * from ssp_campaign_daily_dwr_view;
--where b_date <'2017-09-10';

