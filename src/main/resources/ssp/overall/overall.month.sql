--*******************************************************************************************************
-- Overall for month DM - START
--*******************************************************************************************************
-- ssp_report_overall_dm_month
drop view if exists ssp_report_overall_dm_month;
create view ssp_report_overall_dm_month as
select
    dwr.publisherId   as publisherid,
    p.name            as publishername,
    dwr.appId         as appid,--INT,
    a.name            as appname,
    dwr.countryId     as countryid,--INT,
    c.name            as countryname,
    dwr.campaignId    as campaignid,--INT,
    cam.name          as campaignname,
    dwr.offerId       as offerid,--INT,
    o.name            as offername,
    dwr.requestCount  as requestcount,-- BIGINT,
    dwr.sendCount     as sendcount,-- BIGINT,
    dwr.showCount     as showcount,-- BIGINT,
    dwr.clickCount    as clickcount,--BIGINT,
    dwr.feeReportCount as feereportcount,-- BIGINT,         -- 计费条数
    dwr.feeSendCount   as feesendcount,--BIGINT,         -- 计费显示条数
    dwr.feeReportPrice as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
    dwr.feeSendPrice  as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
    dwr.cpcBidPrice   as cpcbidprice,-- DECIMAL(19,10),
    dwr.cpmBidPrice   as cpmbidprice,-- DECIMAL(19,10),
    dwr.conversion    as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    dwr.allConversion as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
    dwr.revenue       as revenue,-- DECIMAL(19,10), -- 收益
    dwr.realRevenue   as realrevenue,-- DECIMAL(19,10) -- 真实收益

    dwr.b_time,
    dwr.l_time,
    dwr.b_date
from (

    SELECT * FROM ssp_report_overall_dwr_month where b_date >=   "2018-01-01"  -- "ssp_report_overall_dwr.b_date: (2018-01-15 - ] ";
    UNION ALL
    SELECT * FROM ssp_report_campaign_dwr_month where  b_date >= "2017-12-01" and b_date <= "2018-01-01"
    UNION ALL
    SELECT * FROM ssp_report_old_dwr_month where b_date <= "2017-12-01"

) dwr
left join publisher p       on p.id = dwr.publisherId
left join campaign cam      on cam.id = dwr.campaignId
left join offer o           on o.id = dwr.offerId
left join app a             on a.id = dwr.appId
left join country c         on c.id = dwr.countryId
left join version_control v on v.version = dwr.versionName
where v.id is not null;

-- Campaign for month [2017-12-18 - 2018-01-15]
drop table if exists ssp_report_campaign_dwr_month;
create table ssp_report_campaign_dwr_month like ssp_report_overall_dwr_month_base;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_report_campaign_dwr_month
select
    publisherid,
    appid,
    countryid,
    campaignid,
    offerid,
    versionName,
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

    sum(feeCpcTimes)             as feecpctimes,--cpc计费转化数
    sum(feeCpmTimes)             as feecpmtimes,
    sum(feeCpaTimes)             as feecpatimes,
    sum(feeCpaSendTimes)         as feecpasendtimes,--cpc 计费下发数
    sum(feeCpcReportPrice)       as feecpcreportprice,--cpc 计费上游收益
    sum(feeCpmReportPrice)       as feecpmreportprice,
    sum(feeCpaReportPrice)       as feecpareportprice,
    sum(feeCpcSendPrice)         as feecpcsendprice,
    sum(feeCpmSendPrice)         as feecpmsendprice,
    sum(feeCpaSendPrice)         as feecpasendprice,

    concat(date_format(b_date, 'yyyy-MM-01'), ' 00:00:00') as b_time,
    date_format(l_time, 'yyyy-MM-01 00:00:00')             as l_time,
    date_format(b_date, 'yyyy-MM-01')                      as b_date
from ssp_report_campaign_dwr
where b_date >= "2017-12-18" and b_date <= "2018-01-15"
group by
    publisherid                                           ,
    appid                                                 ,
    countryid                                             ,
    campaignid                                            ,
    offerid                                               ,
    versionname                                           ,
    concat(date_format(b_date, 'yyyy-MM-01'), ' 00:00:00'), -- as b_time
    date_format(l_time, 'yyyy-MM-01 00:00:00')            ,
    date_format(b_date, 'yyyy-MM-01');

-- Old system stat data for month [ - 2017-12-18)
set spark.default.parallelism = 1;
set spark.sql.shuffle.partitions = 1;
drop table if exists ssp_report_old_dwr_month;
create table ssp_report_old_dwr_month like ssp_report_overall_dwr_month_base;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_report_old_dwr_month
select
    publisherid,
    appid,
    countryid,
    campaignid,
    offerid,
    versionName,
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

    sum(feeCpcTimes)             as feecpctimes,--cpc计费转化数
    sum(feeCpmTimes)             as feecpmtimes,
    sum(feeCpaTimes)             as feecpatimes,
    sum(feeCpaSendTimes)         as feecpasendtimes,--cpc 计费下发数
    sum(feeCpcReportPrice)       as feecpcreportprice,--cpc 计费上游收益
    sum(feeCpmReportPrice)       as feecpmreportprice,
    sum(feeCpaReportPrice)       as feecpareportprice,
    sum(feeCpcSendPrice)         as feecpcsendprice,
    sum(feeCpmSendPrice)         as feecpmsendprice,
    sum(feeCpaSendPrice)         as feecpasendprice,

    date_format(b_time, 'yyyy-MM-01 00:00:00')             as b_time,
    date_format(l_time, 'yyyy-MM-01 00:00:00')             as l_time,
    date_format(b_date, 'yyyy-MM-01')                      as b_date
from (
    select
        t.publisherId           as publisherid,--INT,
        t.appid                 as appid,--INT,
        t.countryId             as countryid,--INT,
        t.campaignId            as campaignid,--INT,
        t.offerId               as offerid,--INT,
        t.sdkversion            as versionname,

        0                       as requestcount,-- BIGINT,
        t.sendcount             as sendcount,-- BIGINT,
        t.showcount             as showcount,-- BIGINT,
        t.clickcount            as clickcount,--BIGINT,

        t.feecount              as feereportcount,-- BIGINT,         -- 计费条数
        t.sendfeecount          as feesendcount,--BIGINT,         -- 计费显示条数
        t.fee                   as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
        t.sendfee               as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
        0                       as cpcbidprice,-- DECIMAL(19,10),
        0                       as cpmbidprice,-- DECIMAL(19,10),
        t.feecount              as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
        t.feecount              as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
        t.sendfee               as revenue,-- DECIMAL(19,10), -- 收益
        t.fee                   as realrevenue,-- DECIMAL(19,10) -- 真实收益

        0                      as feecpctimes,--cpc计费转化数
        0                      as feecpmtimes,
        0                      as feecpatimes,
        0                      as feecpasendtimes,--cpc 计费下发数
        0                      as feecpcreportprice,--cpc 计费上游收益
        0                      as feecpmreportprice,
        0                      as feecpareportprice,
        0                      as feecpcsendprice,
        0                      as feecpmsendprice,
        0                      as feecpasendprice,
        concat(date_format(statDate, 'yyyy-MM-01'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        date_format(t.statdate, 'yyyy-MM-01')                    as b_date
    from DATA_STAT t  -- overall report
    where t.statdate < "2017-12-18" and (t.statdate < "2017-05-01" or "2017-07-01" <= t.statdate) -- DATA_STAT [5月 - 6月]的数据有问题，用ADVER_STAT代替
    UNION ALL
    select
        -1                      as publisherid,--INT,
        -1                      as appid,--INT,
        t.countryId             as countryid,--INT,
        t.campaignId            as campaignid,--INT,
        t.offerId               as offerid,--INT,
        t.sdkversion            as versionname,

        0                       as requestcount,-- BIGINT,
        t.sendcount             as sendcount,-- BIGINT,
        t.showcount             as showcount,-- BIGINT,
        t.clickcount            as clickcount,--BIGINT,

        t.feecount              as feereportcount,-- BIGINT,         -- 计费条数
        t.sendfeecount          as feesendcount,--BIGINT,         -- 计费显示条数
        t.fee                   as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
        t.sendfee               as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
        0                       as cpcbidprice,-- DECIMAL(19,10),
        0                       as cpmbidprice,-- DECIMAL(19,10),
        t.feecount              as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
        t.feecount              as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
        t.sendfee               as revenue,-- DECIMAL(19,10), -- 收益
        t.fee                   as realrevenue,-- DECIMAL(19,10) -- 真实收益

        0                      as feecpctimes,--cpc计费转化数
        0                      as feecpmtimes,
        0                      as feecpatimes,
        0                      as feecpasendtimes,--cpc 计费下发数
        0                      as feecpcreportprice,--cpc 计费上游收益
        0                      as feecpmreportprice,
        0                      as feecpareportprice,
        0                      as feecpcsendprice,
        0                      as feecpmsendprice,
        0                      as feecpasendprice,
        concat(date_format(statDate, 'yyyy-MM-01'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        date_format(t.statdate, 'yyyy-MM-01')                    as b_date
    from ADVER_STAT t  -- campaign report
    where t.statdate < "2017-12-18" and "2017-05-01" <=  t.statdate  and t.statdate < "2017-07-01" -- DATA_STAT [5月 - 6月]的数据有问题，用ADVER_STAT代替
    UNION ALL
    select
        x.publisherId   as publisherid,--INT,
        x.appId         as appid,--INT,
        x.countryId     as countryid,--INT,
        -1              as campaignid,--INT,
        -1              as offerid,--INT,
        x.sdkversion    as versionname,
        x.totalcount    as requestcount,-- BIGINT,
        0               as sendcount,-- BIGINT,
        0               as showcount,-- BIGINT,
        0               as clickcount,--BIGINT,

        0               as feereportcount,-- BIGINT,         -- 计费条数
        0               as feesendcount,--BIGINT,         -- 计费显示条数
        0               as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
        0               as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
        0               as cpcbidprice,-- DECIMAL(19,10),
        0               as cpmbidprice,-- DECIMAL(19,10),
        0               as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
        0               as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
        0               as revenue,-- DECIMAL(19,10), -- 收益
        0               as realrevenue,-- DECIMAL(19,10) -- 真实收益

        0               as feecpctimes,--cpc计费转化数
        0               as feecpmtimes,
        0               as feecpatimes,
        0               as feecpasendtimes,--cpc 计费下发数
        0               as feecpcreportprice,--cpc 计费上游收益
        0               as feecpmreportprice,
        0               as feecpareportprice,
        0               as feecpcsendprice,
        0               as feecpmsendprice,
        0               as feecpasendprice,
        concat(date_format(statdate, 'yyyy-MM-01'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        date_format(x.statdate, 'yyyy-MM-01')                    as b_date
    from data_fill_stat x
    where x.statdate < "2017-12-18"
) t
group by
    publisherid                                             ,
    appid                                                   ,
    countryid                                               ,
    campaignid                                              ,
    offerid                                                 ,
    versionname                                             ,
    date_format(b_time, 'yyyy-MM-01 00:00:00')              ,
    date_format(l_time, 'yyyy-MM-01 00:00:00')              ,
    date_format(b_date, 'yyyy-MM-01')
    limit 2147483647;
--*******************************************************************************************************
-- Overall for month DM - END
--*******************************************************************************************************

