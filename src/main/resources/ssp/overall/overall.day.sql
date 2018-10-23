--*******************************************************************************************************
-- Overall for day DM - START
--*******************************************************************************************************
-- ssp_report_overall_dm_day (用于代替原ssp_report_campaign_dmssp_report_publisher_dm表)
drop view if exists ssp_report_overall_dm_day;
create view ssp_report_overall_dm_day as
select
    coalesce(dwr.publisherId, a.publisherId) as publisherid , --新增/活跃用户数据没有含publisherId，需通过配置表关联得到
    dwr.appid,--INT,
    dwr.countryId     as countryid,--INT,
    dwr.carrierId     as carrierid,--INT,
    dwr.adType        as adtype,--INT,
    dwr.campaignId    as campaignid,--INT,
    dwr.offerId       as offerid,--INT,
    dwr.imageId       as imageid,--INT,
    dwr.affSub        as affsub,-- STRING,
    dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3

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

    dwr.newCount      as newcount,        -- 新增用户数 for publisher report
    dwr.activeCount   as activecount,     -- 活跃用户数 for publisher report

    dwr.thirdclickcount         as thirdclickcount,  -- 第三方点击
    dwr.thirdsendfee            as thirdsendfee,     -- 第三方显示计费
    dwr.thirdfee                as thirdfee,         -- 第三方计费

    dwr.feeCpcTimes             as feecpctimes,--cpc计费转化数
    dwr.feeCpmTimes             as feecpmtimes,
    dwr.feeCpaTimes             as feecpatimes,
    dwr.feeCpaSendTimes         as feecpasendtimes,--cpc 计费下发数
    dwr.feeCpcReportPrice       as feecpcreportprice,--cpc 计费上游收益
    dwr.feeCpmReportPrice       as feecpmreportprice,
    dwr.feeCpaReportPrice       as feecpareportprice,
    dwr.feeCpcSendPrice         as feecpcsendprice,
    dwr.feeCpmSendPrice         as feecpmsendprice,
    dwr.feeCpaSendPrice         as feecpasendprice,

    dwr.b_time,
    dwr.b_date,
    dwr.l_time,

    --names:
    coalesce(p.amId, a_p.amId)              as publisheramid,
    coalesce(p_am.name, ap_am.name)         as publisheramname,
    ad.amId           as advertiseramid,
    a_am.name         as advertiseramname,
    a.mode            as appmodeid,
    m.name            as appmodename,
    cam.adCategory1   as adcategory1id, --INT 关联campaign.adCategory1
    adc.name          as adcategory1name,
    cam.name          as campaignname,
    cam.adverId       as adverid,
    ad.name           as advername,
    o.optStatus       as offeroptstatus,
    o.name            as offername,
    coalesce(p.name, a_p.name)    as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,
    dwr.adType        as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
--    dwr.versionName   as versionname,
    coalesce(p_am.proxyId, ap_am.proxyId)    as publisherproxyid,
    cast(null as string)    as data_type,
    c.alpha2_code           as countrycode,

    co.id    as companyid,
    co.name  as companyname,

    --新增/活跃用户数据没有含publisherId，需通过配置表关联得到
    coalesce(p.ampaId, a_p.ampaId) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    coalesce(p_amp.name, ap_amp.name) as publisherampaname,
--    p.ampaId as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
--    p_amp.name as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name          as advertiseramaaname
from (

    SELECT * FROM ssp_report_overall_dwr_day_with_third
    UNION ALL
    SELECT * FROM ssp_report_old_dwr_day

) dwr
left join campaign cam      on cam.id = dwr.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join employee a_am     on a_am.id = ad.amid
left join offer o           on o.id = dwr.offerId
left join publisher p       on p.id = dwr.publisherId
left join employee p_am     on p_am.id = p.amid
left join app a             on a.id = dwr.appId
left join app_mode m        on m.id = a.mode
left join country c         on c.id = dwr.countryId
left join carrier ca        on ca.id = dwr.carrierId
left join ad_type adt       on adt.id = dwr.adType
left join version_control v on v.version = dwr.versionName
left join ad_category1 adc  on adc.id =  cam.adCategory1

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaid
left join employee ap_amp   on ap_amp.id  = a_p.ampaid  -- 打开用于用户数渠道信息关联
left join employee a_ama    on a_ama.id = ad.amaaid;
-- where v.id is not null;
 --用户数据没有version相关信息

--
drop view if exists ssp_report_overall_dwr_day_with_third;
create view ssp_report_overall_dwr_day_with_third as
select
    dwr.publisherId   as publisherid,--INT,
    dwr.appId         as appid,--INT,
    dwr.countryId     as countryid,--INT,
    dwr.carrierId     as carrierid,--INT,
    dwr.adType        as adtype,--INT,
    dwr.campaignId    as campaignid,--INT,
    dwr.offerId       as offerid,--INT,
    dwr.imageId       as imageid,--INT,
    dwr.affSub        as affsub,-- STRING,
    dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3

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

--    临时
--    0      as newcount,        -- 新增用户数 for publisher report
--    0      as activecount,     -- 活跃用户数 for publisher report
    dwr.newCount      as newcount,        -- 新增用户数 for publisher report
    dwr.activeCount   as activecount,     -- 活跃用户数 for publisher report

    0 as thirdclickcount,  -- 第三方点击
    0 as thirdsendfee,     -- 第三方显示计费
    0 as thirdfee,         -- 第三方计费

    feeCpcTimes             as feecpctimes,--cpc计费转化数
    feeCpmTimes             as feecpmtimes,
    feeCpaTimes             as feecpatimes,
    feeCpaSendTimes         as feecpasendtimes,--cpc 计费下发数
    feeCpcReportPrice       as feecpcreportprice,--cpc 计费上游收益
    feeCpmReportPrice       as feecpmreportprice,
    feeCpaReportPrice       as feecpareportprice,
    feeCpcSendPrice         as feecpcsendprice,
    feeCpmSendPrice         as feecpmsendprice,
    feeCpaSendPrice         as feecpasendprice,

    dwr.b_time,
    dwr.l_time,
    dwr.b_date
from ssp_report_overall_dwr_day dwr
where b_date > "2018-01-15"
UNION ALL
SELECT
    null                    as publisherid,--INT,
    x.appid                 as appid,--INT,
    x.countryId             as countryid,--INT,
    -1                      as carrierid,
    -1                      as adtype,
    -1                      as campaignid,--INT,
    -1                      as offerid,
    -1                      as imageid,
    "third-income"          as affsub,
    "third-income"          as versionname, -- STRING,         -- eg: v1.2.3

    0                       as requestcount,-- BIGINT,
    0                       as sendcount,-- BIGINT,
    0                       as showcount,-- BIGINT,
    0                       as clickcount,--BIGINT,

    0                       as feereportcount,-- BIGINT,         -- 计费条数
    0                       as feesendcount,--BIGINT,         -- 计费显示条数
    0                       as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
    0                       as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
    0                       as cpcbidprice,-- DECIMAL(19,10),
    0                       as cpmbidprice,-- DECIMAL(19,10),
    0                       as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    0                       as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
    0                       as revenue,-- DECIMAL(19,10), -- 收益
    0                       as realrevenue,-- DECIMAL(19,10) -- 真实收益

    0                       as newcount,        -- 新增用户数 for publisher report
    0                       as activecount,     -- 活跃用户数 for publisher report

    pv                      as thirdclickcount,  -- 第三方点击
    thirdsendfee            as thirdsendfee,     -- 第三方显示计费
    thirdfee                as thirdfee,         -- 第三方计费

    0                       as feecpctimes,--cpc计费转化数
    0                       as feecpmtimes,
    0                       as feecpatimes,
    0                       as feecpasendtimes,--cpc 计费下发数
    0                       as feecpcreportprice,--cpc 计费上游收益
    0                       as feecpmreportprice,
    0                       as feecpareportprice,
    0                       as feecpcsendprice,
    0                       as feecpmsendprice,
    0                       as feecpasendprice,

    concat(date_format(statdate, 'yyyy-MM-dd'), ' 00:00:00') as b_time,
    '2000-01-01 00:00:00'                                    as l_time,
    x.statdate                                               as b_date
from ssp_report_publisher_third_income x;
--
--drop table if exists ssp_report_old_dwr_day;
--create table ssp_report_old_dwr_day(
--    publisherid         int             ,
--    appid               int             ,
--    countryid           int             ,
--    carrierid           int             ,
--    adtype              int             ,
--    campaignid          int             ,
--    offerid             int             ,
--    imageid             int             ,
--    affsub              string          ,
--    versionname         string          ,
--
--    requestcount        bigint          ,
--    sendcount           bigint          ,
--    showcount           bigint          ,
--    clickcount          bigint          ,
--    feereportcount      bigint          ,
--    feesendcount        bigint          ,
--    feereportprice      decimal(20,10)  ,
--    feesendprice        decimal(20,10)  ,
--    cpcbidprice         decimal(20,10)  ,
--    cpmbidprice         decimal(20,10)  ,
--    conversion          bigint          ,
--    allconversion       bigint          ,
--    revenue             decimal(20,10)  ,
--    realrevenue         decimal(20,10)  ,
--
--    newcount            bigint          ,
--    activecount         bigint          ,
--    thirdclickcount     int             ,
--    thirdsendfee        decimal(11,1)   ,
--    thirdfee            decimal(11,1)   ,
--
--    feecpctimes         bigint          ,
--    feecpmtimes         bigint          ,
--    feecpatimes         bigint          ,
--    feecpasendtimes     bigint          ,
--    feecpcreportprice   decimal(20,10)  ,
--    feecpmreportprice   decimal(20,10)  ,
--    feecpareportprice   decimal(20,10)  ,
--    feecpcsendprice     decimal(20,10)  ,
--    feecpmsendprice     decimal(20,10)  ,
--    feecpasendprice     decimal(20,10)
--)
--PARTITIONED BY (b_time STRING, l_time STRING, b_date STRING)
--STORED AS ORC;


set spark.default.parallelism = 1;
set spark.sql.shuffle.partitions = 1;
set hive.exec.dynamic.partition.mode=nonstrict;

--insert overwrite table ssp_report_old_dwr_day
--select * from ssp_report_old_dwr_day_view where b_date >="2018-01-01";

--insert overwrite table ssp_report_old_dwr_day
--select * from ssp_report_old_dwr_day_view where b_date >= "2017-08-01" and b_date < "2018-01-01";
--
--insert overwrite table ssp_report_old_dwr_day
--select * from ssp_report_old_dwr_day_view where b_date >= "2017-04-01" and b_date < "2017-08-01";
--
--insert overwrite table ssp_report_old_dwr_day
--select * from ssp_report_old_dwr_day_view where b_date < "2017-04-01";

--create view ssp_report_old_dwr_day_view as

drop view if exists ssp_report_old_dwr_day;
create view ssp_report_old_dwr_day as
select
    dwr.publisherId   as publisherid,--INT,
    dwr.appId         as appid,--INT,
    dwr.countryId     as countryid,--INT,
    dwr.carrierId     as carrierid,--INT,
    dwr.adType        as adtype,--INT,
    dwr.campaignId    as campaignid,--INT,
    dwr.offerId       as offerid,--INT,
    dwr.imageId       as imageid,--INT,
    dwr.affSub        as affsub,-- STRING,
    dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3

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

    sum(newcount)               as newcount,
    sum(activecount)            as activecount,
    sum(thirdclickcount)        as thirdclickcount,
    sum(thirdsendfee)           as thirdsendfee,
    sum(thirdfee)               as thirdfee,

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
    b_time,
    l_time,
    b_date
from(
    select
        dwr.publisherId   as publisherid,--INT,
        dwr.appId         as appid,--INT,
        dwr.countryId     as countryid,--INT,
        dwr.carrierId     as carrierid,--INT,
        dwr.adType        as adtype,--INT,
        dwr.campaignId    as campaignid,--INT,
        dwr.offerId       as offerid,--INT,
        dwr.imageId       as imageid,--INT,
        dwr.affSub        as affsub,-- STRING,
        dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3

        dwr.requestCount  as requestcount,-- BIGINT,
        dwr.sendCount     as sendcount,-- BIGINT,
        dwr.showCount     as showcount,-- BIGINT,
        dwr.clickCount    as clickcount,--BIGINT,

        dwr.feeReportCount as feereportcount,-- BIGINT,         -- 计费条数
        dwr.feeSendCount   as feesendcount,--BIGINT,         -- 计费显示条数
        dwr.feeReportPrice as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
        dwr.feeSendPrice   as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)

        dwr.cpcBidPrice    as cpcbidprice,-- DECIMAL(19,10),
        dwr.cpmBidPrice    as cpmbidprice,-- DECIMAL(19,10),
        dwr.conversion     as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
        dwr.allConversion  as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
        dwr.revenue        as revenue,-- DECIMAL(19,10), -- 收益
        dwr.realRevenue    as realrevenue,-- DECIMAL(19,10) -- 真实收益

        0                  as newcount,        -- 新增用户数 for publisher report
        0                  as activecount,     -- 活跃用户数 for publisher report

        0                  as thirdclickcount,  -- 第三方点击
        0.0                as thirdsendfee,     -- 第三方显示计费
        0.0                as thirdfee,         -- 第三方计费

        feeCpcTimes             as feecpctimes,--cpc计费转化数
        feeCpmTimes             as feecpmtimes,
        feeCpaTimes             as feecpatimes,
        feeCpaSendTimes         as feecpasendtimes,--cpc 计费下发数
        feeCpcReportPrice       as feecpcreportprice,--cpc 计费上游收益
        feeCpmReportPrice       as feecpmreportprice,
        feeCpaReportPrice       as feecpareportprice,
        feeCpcSendPrice         as feecpcsendprice,
        feeCpmSendPrice         as feecpmsendprice,
        feeCpaSendPrice         as feecpasendprice,

        concat(date_format(b_date, 'yyyy-MM-dd'), ' 00:00:00') AS b_time,
        dwr.l_time,
        dwr.b_date
    from ssp_report_campaign_dwr dwr
    where "2017-12-18" <= b_date and b_date <= "2018-01-15"
    UNION ALL
    select
        t.publisherId           as publisherid,--INT,
        t.appid                 as appid,--INT,
        t.countryId             as countryid,--INT,
        t.carrierid             as carrierid,
        t.adType                as adtype,
        t.campaignId            as campaignid,--INT,
        t.offerId               as offerid,
        -1                      as imageid,
        ""                      as affsub,
        t.sdkversion            as versionname, -- STRING,         -- eg: v1.2.3

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
        t.feecount              as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的(暂时没用到)
        t.sendfee               as revenue,-- DECIMAL(19,10), -- 收益
        t.fee                   as realrevenue,-- DECIMAL(19,10) -- 真实收益

        0                       as newcount,        -- 新增用户数 for publisher report
        0                       as activecount,     -- 活跃用户数 for publisher report

        0                       as thirdclickcount,  -- 第三方点击
        0                       as thirdsendfee,     -- 第三方显示计费
        0                       as thirdfee,         -- 第三方计费

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

        concat(date_format(t.statDate, 'yyyy-MM-dd'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        t.statdate                                               as b_date
    from DATA_STAT t  -- overall report
    where t.statdate < "2017-12-18" and (t.statdate < "2017-05-01" or "2017-07-01" <= t.statdate) -- DATA_STAT [5月 - 6月]的数据有问题，用ADVER_STAT代替
    UNION ALL
    select
        -1                      as publisherid,--INT,
        -1                      as appid,--INT,
        t.countryId             as countryid,--INT,
        t.carrierid             as carrierid,
        t.adType                as adtype,
        t.campaignId            as campaignid,--INT,
        t.offerId               as offerid,--INT,
        -1                      as imageid,
        ""                      as affsub,
        t.sdkversion            as versionname, -- eg: v1.2.3

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

        0                       as newcount,        -- 新增用户数 for publisher report
        0                       as activecount,     -- 活跃用户数 for publisher report

        0                       as thirdclickcount,  -- 第三方点击
        0                       as thirdsendfee,     -- 第三方显示计费
        0                       as thirdfee,         -- 第三方计费

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
    where t.statdate < "2017-12-18" and ("2017-05-01" <=  t.statdate  and t.statdate < "2017-07-01") -- DATA_STAT [5月 - 6月]的数据有问题，用ADVER_STAT代替

    UNION ALL
    select
        x.publisherId           as publisherid,--INT,
        x.appid                 as appid,--INT,
        x.countryId             as countryid,--INT,
        x.carrierid             as carrierid,
        x.adtype                as adtype,
        -1                      as campaignid,--INT,
        -1                      as offerid,
        -1                      as imageid,
        ""                      as affsub,
        x.sdkversion            as versionname, -- STRING,         -- eg: v1.2.3

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

        0               as newcount,        -- 新增用户数 for publisher report
        0               as activecount,     -- 活跃用户数 for publisher report

        0               as thirdclickcount,  -- 第三方点击
        0               as thirdsendfee,     -- 第三方显示计费
        0               as thirdfee,         -- 第三方计费

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

        concat(date_format(statdate, 'yyyy-MM-dd'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        x.statdate                                               as b_date
    from data_fill_stat x
    where x.statdate < "2017-12-18"
--    UNION ALL
    -- user stat START
--    select
--        dwr.publisherId   as publisherid,--INT,
--        dwr.appId         as appid,--INT,
--        dwr.countryId     as countryid,--INT,
--        dwr.carrierId     as carrierid,--INT,
--        dwr.adType        as adtype,--INT,
--        dwr.campaignId    as campaignid,--INT,
--        dwr.offerId       as offerid,--INT,
--        dwr.imageId       as imageid,--INT,
--        dwr.affSub        as affsub,-- STRING,
--        dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3
--
--        0                 as requestcount,-- BIGINT,
--        0                 as sendcount,-- BIGINT,
--        0                 as showcount,-- BIGINT,
--        0                 as clickcount,--BIGINT,
--        0                 as feereportcount,-- BIGINT,         -- 计费条数
--        0                 as feesendcount,--BIGINT,         -- 计费显示条数
--        0                 as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
--        0                 as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
--        0                 as cpcbidprice,-- DECIMAL(19,10),
--        0                 as cpmbidprice,-- DECIMAL(19,10),
--        0                 as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
--        0                 as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
--        0                 as revenue,-- DECIMAL(19,10), -- 收益
--        0                 as realrevenue,-- DECIMAL(19,10) -- 真实收益
--
----      临时
----        0      as newcount,        -- 新增用户数 for publisher report
----        0   as activecount,     -- 活跃用户数 for publisher report
--        dwr.newCount      as newcount,        -- 新增用户数 for publisher report
--        dwr.activeCount   as activecount,     -- 活跃用户数 for publisher report
--
--        0 as thirdclickcount,  -- 第三方点击
--        0 as thirdsendfee,     -- 第三方显示计费
--        0 as thirdfee,         -- 第三方计费
--
--        0                as feecpctimes,--cpc计费转化数
--        0                as feecpmtimes,
--        0                as feecpatimes,
--        0                as feecpasendtimes,--cpc 计费下发数
--        0                as feecpcreportprice,--cpc 计费上游收益
--        0                as feecpmreportprice,
--        0                as feecpareportprice,
--        0                as feecpcsendprice,
--        0                as feecpmsendprice,
--        0                as feecpasendprice,
--
--        dwr.b_time,
--        dwr.l_time,
--        dwr.b_date
--    from ssp_report_overall_dwr_day dwr
--    where b_date > "2018-02-07"  -- 单独为用户数据
    UNION ALL
    select
        dwr.publisherId   as publisherid,--INT,
        dwr.appId         as appid,--INT,
        dwr.countryId     as countryid,--INT,
        dwr.carrierId     as carrierid,--INT,
        -1                as adtype,--INT,
        -1                as campaignid,--INT,
        -1                as offerid,--INT,
        -1                as imageid,--INT,
        dwr.affSub        as affsub,-- STRING,
        dwr.versionName   as versionname,-- STRING,         -- eg: v1.2.3

        0                 as requestcount,-- BIGINT,
        0                 as sendcount,-- BIGINT,
        0                 as showcount,-- BIGINT,
        0                 as clickcount,--BIGINT,

        0                 as feereportcount,-- BIGINT,         -- 计费条数
        0                 as feesendcount,--BIGINT,         -- 计费显示条数
        0                 as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
        0                 as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)

        0                 as cpcbidprice,-- DECIMAL(19,10),
        0                 as cpmbidprice,-- DECIMAL(19,10),
        0                 as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
        0                 as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
        0                 as revenue,-- DECIMAL(19,10), -- 收益
        0                 as realrevenue,-- DECIMAL(19,10) -- 真实收益

        dwr.newCount       as newcount,        -- 新增用户数 for publisher report
        dwr.activeCount    as activecount,     -- 活跃用户数 for publisher report

        0                  as thirdclickcount,  -- 第三方点击
        0                  as thirdsendfee,     -- 第三方显示计费
        0                  as thirdfee,         -- 第三方计费

        0                  as feecpctimes,--cpc计费转化数
        0                  as feecpmtimes,
        0                  as feecpatimes,
        0                  as feecpasendtimes,--cpc 计费下发数
        0                  as feecpcreportprice,--cpc 计费上游收益
        0                  as feecpmreportprice,
        0                  as feecpareportprice,
        0                  as feecpcsendprice,
        0                  as feecpmsendprice,
        0                  as feecpasendprice,

        concat(date_format(b_date, 'yyyy-MM-dd'), ' 00:00:00') AS b_time,
        dwr.l_time,
        dwr.b_date
    from ssp_report_publisher_dwr dwr
    -- ssp_report_overall_dwr_day 2018-02-07 之后才有用户数据,此前的数据需要从ssp_report_publisher_dwr中取
    where "2017-12-18" <= b_date and b_date <= "2018-02-07"
    UNION ALL
    select
        null                    as publisherid,--INT,
        x.appid                 as appid,--INT,
        x.countryId             as countryid,--INT,
        -1                      as carrierid,
        -1                      as adtype,
        -1                      as campaignid,--INT,
        -1                      as offerid,
        -1                      as imageid,
        ""                      as affsub,
        x.sdkversion            as versionname, -- STRING,         -- eg: v1.2.3

        0               as requestcount,-- BIGINT,
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

        x.userCount     as newcount,        -- 新增用户数 for publisher report
        x.activeCount   as activecount,     -- 活跃用户数 for publisher report

        0               as thirdclickcount,  -- 第三方点击
        0               as thirdsendfee,     -- 第三方显示计费
        0               as thirdfee,         -- 第三方计费

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

        concat(date_format(statdate, 'yyyy-MM-dd'), ' 00:00:00') as b_time,
        '2000-01-01 00:00:00'                                    as l_time,
        x.statdate                                               as b_date
    from publisher_stat x
    where x.statdate < "2017-12-18"
) dwr
group by
    dwr.publisherId,
    dwr.appId,
    dwr.countryId,
    dwr.carrierId,
    dwr.adType,
    dwr.campaignId,
    dwr.offerId,
    dwr.imageId,
    dwr.affSub,
    dwr.versionName,
    b_time,
    dwr.l_time,
    dwr.b_date;
-- usr stat END
--*******************************************************************************************************
-- Overall for day DM - END
--*******************************************************************************************************




