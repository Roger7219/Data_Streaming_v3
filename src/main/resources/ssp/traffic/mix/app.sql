-- FOR Handler
-- 渠道ID，AppId接口
create table ssp_app_dwr(
    publisherId     INT,
    appId           INT,
    sendCount       BIGINT,
    showCount       BIGINT,
    clickCount      BIGINT,
    conversion      BIGINT,         --计费条数
    sendsConversion BIGINT,         --显示计费条数
    cost            DECIMAL(19,10), --计费金额
    sendsCost       DECIMAL(19,10)  --显示计费金额
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

create table ssp_app_totalcost_dwr(
    appId       INT,
    cost        DECIMAL(19,10) --计费金额
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


-- Hive FOR Bigquery
drop view if exists ssp_app_dm;
create view ssp_app_dm as
select
    dwr.*,
    p.name          as publishername,
    a.name          as appname
from ssp_app_dwr dwr
left join publisher p on p.id = dwr.publisherId
left join app a       on a.id = dwr.appId;

------------------------------------------------------------------------------------------------------------------------
-- 离线统计
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_app_totalcost_dwr_view ;
create view ssp_app_totalcost_dwr_view as
select
    appId,
    sum(cost),
    l_time,
    b_date
from (
    select
        subId as appId,
        sum(reportPrice) as cost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01') as b_date
    from ssp_fee_dwr
    where second(l_time) = 0
    group by
        subId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')

    union all
    select
        subId as appId,
        sum(cpmBidPrice) as cost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01') as b_date
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        subId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')

    union all
    select
        subId as appId,
        sum(cpcBidPrice) as cost,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01') as b_date
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        subId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')
) t0
group by
    appId,
    l_time,
    b_date ;



--create table ssp_app_totalcost_dwr_tmp like ssp_app_totalcost_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ssp_app_totalcost_dwr partition(l_time, b_date)
select * from ssp_app_totalcost_dwr_view;
--where b_date <='2017-10-01';

drop view if exists ssp_app_dwr_view;
create view ssp_app_dwr_view as
select
    publisherId,
    appId,
    sum(sendCount)  as sendCount,
    sum(showCount)  as showCount,
    sum(clickCount) as clickCount,
    sum(conversion) as conversion,
    sum(sendsConversion) as sendsConversion,
    sum(cost)            as cost,
    sum(sendsCost)       as sendsCost,
    l_time,
    b_date
from (
    select
        publisherId,
        subId as appId,
        0 as sendCount,
        0 as showCount,
        0 as clickCount,
        sum(times) as conversion,
        sum(sendTimes) as sendsConversion,
        sum(reportPrice) as cost,
        sum(sendPrice) as sendsCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fee_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    union all
    select
        publisherId,
        subId            as appId,
        0                as sendCount,
        0                as showCount,
        sum(times)       as clickCount,
        0                as conversion,
        0                as sendsConversion,
        sum(cpcBidPrice) as cost,
        sum(cpcSendPrice)as sendsCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    union all
    select
        publisherId       as publisherId,
        subId             as appId,
        0                 as sendCount,
        sum(times)        as showCount,
        0                 as clickCount,
        0                 as conversion,
        0                 as sendsConversion,
        sum(cpmBidPrice)  as cost,
        sum(cpmSendPrice) as sendsCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    union all
    select
        publisherId as publisherId,
        subId       as appId,
        sum(times)  as sendCount,
        0           as showCount,
        0           as clickCount,
        0           as conversion,
        0           as sendsConversion,
        0           as cost,
        0          as sendsCost,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_send_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date
 )t0
group by
    publisherId,
    appId,
    l_time,
    b_date;


--create table ssp_app_dwr_tmp like ssp_app_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ssp_app_dwr partition(l_time, b_date)
select * from ssp_app_dwr_view;
--where b_date <='2017-10-01';



--
--create table
--  publisherId,--INT,
--  appId      ,--INT,
--  countryId  ,--INT,
--  carrierId  ,--INT,
--  adType     ,--INT,
--  campaignId ,--INT,
--  offerId    ,--INT,
--  imageId    ,--INT,
--  affSub     ,-- STRING,
--  requestCount ,-- BIGINT,
--  sendCount    ,-- BIGINT,
--  showCount    ,-- BIGINT,
--  clickCount   ,--BIGINT,
--  feeReportCount,-- BIGINT,         -- 计费条数
--  feeSendCount  ,--BIGINT,         -- 计费显示条数
--  feeReportPrice,-- DECIMAL(19,10), -- 计费金额(真实收益)
--  feeSendPrice  ,-- DECIMAL(19,10), -- 计费显示金额(收益)
--  cpcBidPrice   ,-- DECIMAL(19,10),
--  cpmBidPrice   ,-- DECIMAL(19,10),
--  conversion    ,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
--  allConversion ,-- BIGINT,         -- 转化数，含展示和点击产生的
--  revenue       ,-- DECIMAL(19,10), -- 收益
--  realRevenue   ,-- DECIMAL(19,10) -- 真实收益
--  publisheramid,
--  publisheramname,
--  advertiseramid,
--  advertiseramname,
--  appmodeid,
--  appmodename,
--  adcategory1id, --INT 关联campaign.adCategory1
--  adcategory1name,
--  campaignname,
--  adverid,
--  advername,
--  offeroptstatus,
--  offername,
--  publishername,
--  appname,
--  countryname,
--  carriername,
--  adtypeid,   --adFormatId
--  adtypename,
--  versionid,
--  versionname,
--  publisherproxyid,
--  data_type,
--  feecpctimes,--cpc计费转化数
--  feecpmtimes,
--  feecpatimes,
--  feecpasendtimes,--cpc 计费下发数
--  feecpcreportprice,--cpc 计费上游收益
--  feecpmreportprice,
--  feecpareportprice,
--  feecpcsendprice,
--  feecpmsendprice,
--  feecpasendprice,
--  countrycode,
--  l_time,
--  b_date,
--  b_time
--


---

create table ssp_report_campaign_month_dm(
    publisherid     INT,
    publishername   STRING,
    appid           INT,
    appname         STRING,
    adverid         INT,
    advername       STRING,
    carrierid       INT,
    carriername     STRING,
    countryid       INT,
    countryname     STRING,
    campaignid      INT,
    campaignname    STRING,
    offerId         INT,
    offername       STRING,
    requestcount    BIGINT,
    sendcount       BIGINT,
    showcount       BIGINT,
    clickcount      BIGINT,
    feereportcount  bigint,
    feesendcount    BIGINT,
    feereportprice  DOUBLE,
    feesendprice    DOUBLE,
    cpcbidprice     DOUBLE,
    cpmbidprice     DOUBLE,
    conversion      BIGINT,
    allconversion   BIGINT,
    revenue         DOUBLE,
    realrevenue     DOUBLE,
    feecpctimes     BIGINT,
    feecpcsendprice DOUBLE,
    feecpcreportprice   DOUBLE,
    feecpmtimes         BIGINT,
    feecpmreportprice   DOUBLE,
    feecpmsendprice     DOUBLE,
    feecpatimes         BIGINT,
    feecpareportprice   DOUBLE,
    feecpasendtimes     BIGINT,
    feecpasendprice     DOUBLE
)
PARTITIONED BY (b_date STRING)
STORED AS ORC;

set l_time = l_time <= "2017-12-31";
set spark.sql.shuffle.partitions = 1;
set spark.default.parallelism = 1;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table ssp_report_campaign_month_dm
select
    publisherid,
    publishername,
    appid,
    appname,
    adverid,
    advername,
    carrierid,
    carriername,
    countryid,
    countryname,
    campaignid,
    campaignname,
    offerid,
    offername,
    sum(requestcount)   as requestcount,
    sum(sendcount)      as sendcount,
    sum(showcount)      as showcount,
    sum(clickcount)     as clickcount,
    sum(feereportcount) as feereportcount,
    sum(feesendcount)   as feesendcount,
    sum(feereportprice) as feereportprice,
    sum(feesendprice)   as feesendprice,
    sum(cpcbidprice)    as cpcbidprice,
    sum(cpmbidprice)    as cpmbidprice,
    sum(conversion)     as conversion,
    sum(allconversion)  as allconversion,
    sum(revenue)        as revenue,
    sum(realrevenue)    as realrevenue,
    sum(feecpctimes)    as feecpctimes,
    sum(feecpcsendprice)    as feecpcsendprice,
    sum(feecpcreportprice)  as feecpcreportprice,
    sum(feecpmtimes)        as feecpmtimes,
    sum(feecpmreportprice)  as feecpmreportprice,
    sum(feecpmsendprice)    as feecpmsendprice,
    sum(feecpatimes)        as feecpatimes,
    sum(feecpareportprice)  as feecpareportprice,
    sum(feecpasendtimes)    as feecpasendtimes,
    sum(feecpasendprice)    as feecpasendprice,
    date_format(b_date, 'yyyy-MM-01') as b_date
from ssp_report_campaign_dm
where (data_type = 'camapgin' or data_type is null) and ${l_time}
group by
    date_format(b_date, 'yyyy-MM-01'),
    publisherid,
    publishername,
    appid,
    appname,
    adverid,
    advername,
    carrierid,
    carriername,
    countryid,
    countryname,
    campaignid,
    campaignname,
    offerid,
    offername;


select offerid, sum(conversion) from ssp_report_campaign_dm where b_date >= "2017-11-01"  and b_date <= "2017-11-31"
and  (data_type = 'camapgin' or data_type is null)
group by offerid, offername
order by 2 desc limit 10;

select offerid, sum(conversion) from ssp_report_campaign_month_dm where b_date = "2017-11-01"
group by offerid, offername
order by 2 desc limit 10;

select offerid, offername, sum(conversion) from ssp_report_campaign_dm where  (data_type = 'camapgin' or data_type is null)
group by offerid, offername
order by 3 desc limit 10;



select offerid, offername, sum(conversion) from ssp_report_campaign_month_dm
group by offerid, offername
order by 3  desc limit 10;



===

select sum(conversion) from ssp_report_campaign_dm where  (data_type = 'camapgin' or data_type is null)
order by 1 desc limit 10;



select  sum(conversion) from ssp_report_campaign_month_dm
order by 1 desc limit 10;

select  sum(conversion) from ssp_report_campaign_month_dm_base
order by 1 desc limit 10;

select  sum(conversion) from lmm_test1
order by 1 desc limit 10;


set l_time = l_time > "2018-01-01 00:00:00";

set spark.default.parallelism = 3;
set spark.sql.shuffle.partitions = 3;

create view lmm_test1 as
--select * from ssp_report_campaign_month_dm_base
--union all
select
    publisherid,
--    publishername,
    appid,
--    appname,
--    adverid,
--    advername,
    carrierid,
--    carriername,
    countryid,
--    countryname,
    campaignid,
--    campaignname,
    offerid,
--    offername,
    sum(requestcount)   as requestcount,
    sum(sendcount)      as sendcount,
    sum(showcount)      as showcount,
    sum(clickcount)     as clickcount,
    sum(feereportcount) as feereportcount,
    sum(feesendcount)   as feesendcount,
    sum(feereportprice) as feereportprice,
    sum(feesendprice)   as feesendprice,
    sum(cpcbidprice)    as cpcbidprice,
    sum(cpmbidprice)    as cpmbidprice,
    sum(conversion)     as conversion,
    sum(allconversion)  as allconversion,
    sum(revenue)        as revenue,
    sum(realrevenue)    as realrevenue,
    sum(feecpctimes)    as feecpctimes,
    sum(feecpcsendprice)    as feecpcsendprice,
    sum(feecpcreportprice)  as feecpcreportprice,
    sum(feecpmtimes)        as feecpmtimes,
    sum(feecpmreportprice)  as feecpmreportprice,
    sum(feecpmsendprice)    as feecpmsendprice,
    sum(feecpatimes)        as feecpatimes,
    sum(feecpareportprice)  as feecpareportprice,
    sum(feecpasendtimes)    as feecpasendtimes,
    sum(feecpasendprice)    as feecpasendprice,
    date_format(b_date, 'yyyy-MM-01') as b_date
from ssp_report_campaign_dwr
where
 -- (data_type = 'camapgin' or data_type is null) and
  l_time > "2018-01-01 00:00:00"
group by
    date_format(b_date, 'yyyy-MM-01'),
    publisherid,
--    publishername,
    appid,
--    appname,
--    adverid,
--    advername,
    carrierid,
--    carriername,
    countryid,
--    countryname,
    campaignid,
--    campaignname,
    offerid;
--    offername;


















--### FOR Handler
--create table ssp_offer_dwr(
--    offerId         INT,
--    todayClickCount BIGINT,
--    todayShowCount  BIGINT,
--    todayFeeCount       BIGINT,        --当天计费条数
--    todayFee        DECIMAL(19,10), --当天计费条数
--)
--PARTITIONED BY (l_time STRING, b_date STRING)
--STORED AS ORC;
