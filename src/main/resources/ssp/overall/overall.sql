CREATE TABLE ssp_overall_user_new_dwi(
    repeats int,
    rowkey string,
    imei string,
    imsi string,
    createTime string,
    activeTime string,
    appId int,
    model string,
    version string,
    sdkVersion int,
    installType int,
    leftSize string,
    androidId string,
    userAgent string,
    ipAddr string,
    screen string,
    countryId int,
    carrierId int,
    sv string,
    packageName STRING,
    appName STRING,
    affSub string,
    lat string,
    lon string,
    mac1 string,
    mac2 string,
    ssid string,
    lac int,
    cellId int,
    ctype int
)PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;

create table ssp_overall_user_active_dwi like ssp_overall_user_new_dwi;

create table ssp_overall_win_dwi(
        repeats     INT   ,
        rowkey      STRING,
        id          INT   ,
        publisherId INT   , -- + AM
        subId       INT   , -- + APP
        offerId     INT   ,
        campaignId  INT   , -- + campaign > 上级ADVER
        countryId   INT   ,
        carrierId   INT   ,
        deviceType  INT   ,
        userAgent   STRING,
        ipAddr      STRING,
        clickId     STRING,
        price       DOUBLE,
        reportTime  STRING,
        createTime  STRING,
        clickTime   STRING,
        showTime    STRING,
        requestType STRING,
        priceMethod INT   ,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
        bidPrice    DOUBLE,
        adType      INT   ,
        isSend      INT   ,
        reportPrice DOUBLE,
        sendPrice   DOUBLE,
        s1          STRING,
        s2          STRING,
        gaid        STRING,
        androidId   STRING,
        idfa        STRING,
        postBack    STRING,
        sendStatus  INT   ,
        sendTime    STRING,
        sv          STRING,
        imei        STRING,
        imsi        STRING,
        imageId     INT,
        affSub      INT,
        s3          STRING,
        s4          STRING,
        s5          STRING,
        packageName STRING,
        domain      STRING,
        respStatus  INT, --  未下发原因
        winPrice    DOUBLE,
        winTime     STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;


--drop table ssp_overall_fill_dwi;
--drop table ssp_overall_send_dwi;
--drop table ssp_overall_show_dwi;
--drop table ssp_overall_click_dwi;
--drop table ssp_overall_fee_dwi;
--drop table ssp_overall_win_dwi;
--DROP TABLE ssp_report_overall_dwr;

create table ssp_overall_fill_dwi like ssp_overall_win_dwi;
create table ssp_overall_send_dwi like ssp_overall_win_dwi;
create table ssp_overall_show_dwi like ssp_overall_win_dwi;
create table ssp_overall_click_dwi like ssp_overall_win_dwi;
create table ssp_overall_fee_dwi like ssp_overall_win_dwi;

-- Dsp V1.0.1 新增 appPrice, test 字段
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (test INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (test INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (test INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (test INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (test INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (appPrice double); ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (test INT);

--smartlink+ add new columuns
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (ruleId INT);

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (smartId INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (smartId INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (smartId INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (smartId INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (smartId INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (smartId INT);

--postback/event create new table and add new columuns

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (status INT);

ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (status INT);

ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (status INT);

ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (status INT);

ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (status INT);

ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (eventName STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (eventValue INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (refer STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (status INT);


create table if not exists ssp_overall_events_dwi   like ssp_overall_fee_dwi;
create table if not exists ssp_overall_postback_dwi like ssp_overall_fee_dwi;

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (city STRING);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (city STRING);

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (region STRING);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (region STRING);


--18/06/13
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (uid STRING);

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (times INT);

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (time INT);

ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (isNew INT);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (isNew INT);


--postback response信息(2018-06-20)
ALTER TABLE ssp_overall_win_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_show_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_click_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_fee_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_events_dwi ADD COLUMNS (pbResp STRING);
ALTER TABLE ssp_overall_postback_dwi ADD COLUMNS (pbResp STRING);



--添加推荐系统字段(2018-11-29)
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (raterType INT);
ALTER TABLE ssp_overall_fill_dwi ADD COLUMNS (raterId STRING);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (raterType INT);
ALTER TABLE ssp_overall_send_dwi ADD COLUMNS (raterId STRING);


--
--insert overwrite table ssp_report_overall_dwr
--select
--    publisherid ,
--    appid ,
--    countryid ,
--    carrierid ,
--    versionname ,
--    adtype ,
--    campaignid ,
--    offerid ,
--    imageid ,
--    affsub ,
--    sum(requestcount) ,
--    sum(sendcount ),
--    sum(showcount ),
--    sum(clickcount) ,
--    sum(feereportcount) ,
--    sum(feesendcount ),
--    sum(feereportprice) ,
--    sum(feesendprice ),
--    sum(cpcbidprice ),
--    sum(cpmbidprice ),
--    sum(conversion ),
--    sum(allconversion) ,
--    sum(revenue ),
--    sum(realrevenue) ,
--    sum(feecpctimes) ,
--    sum(feecpmtimes) ,
--    sum(feecpatimes) ,
--    sum(feecpasendtimes) ,
--    sum(feecpcreportprice) ,
--    sum(feecpmreportprice) ,
--    sum(feecpareportprice) ,
--    sum(feecpcsendprice) ,
--    sum(feecpmsendprice) ,
--    sum(feecpasendprice) ,
--    packagename ,
--    domain ,
--    operatingsystem ,
--    systemlanguage ,
--    null as devicebrand ,
--    devicetype ,
--    browserkernel ,
--    respStatus,
--    sum(winPrice),
--    sum(winNotices) ,
--    test
--    l_time ,
--    b_date ,
--    b_time
--from ssp_report_overall_dwr
--where l_time < "2017-12-29 17:00:00"
--group by
--    publisherid ,
--    appid ,
--    countryid ,
--    carrierid ,
--    versionname ,
--    adtype ,
--    campaignid ,
--    offerid ,
--    imageid ,
--    affsub ,
--    packagename ,
--    domain ,
--    operatingsystem ,
--    systemlanguage ,
----  devicebrand ,
--    devicetype ,
--    browserkernel ,
--    respStatus ,
--    l_time ,
--    b_date ,
--    b_time,
--    test



-- Dsp V1.0.1 新增 统计维度 test 字段
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (winPrice double);
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (winNotices bigint);
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (test INT);

--smartlink+ add new columuns
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (ruleId INT);
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (smartId INT);
--整合用户数据activeCount、newCount
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (newCount bigint);
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (activeCount bigint);
--新增postbac和events统计维度
ALTER TABLE ssp_report_overall_dwr ADD COLUMNS (eventName STRING);


CREATE TABLE ssp_report_overall_dwr(
        publisherid int,
        appid int,
        countryid int,
        carrierid int,
        versionname string,
        adtype int,
        campaignid int,
        offerid int,
        imageid int,
        affsub string,
        requestcount bigint,
        sendcount bigint,
        showcount bigint,
        clickcount bigint,
        feereportcount bigint,
        feesendcount bigint,
        feereportprice decimal(19,10),
        feesendprice decimal(19,10),
        cpcbidprice decimal(19,10),
        cpmbidprice decimal(19,10),
        conversion bigint,
        allconversion bigint,
        revenue decimal(19,10),
        realrevenue decimal(19,10),
        feecpctimes bigint,
        feecpmtimes bigint,
        feecpatimes bigint,
        feecpasendtimes bigint,
        feecpcreportprice decimal(19,10),
        feecpmreportprice decimal(19,10),
        feecpareportprice decimal(19,10),
        feecpcsendprice decimal(19,10),
        feecpmsendprice decimal(19,10),
        feecpasendprice decimal(19,10),
        packagename string, domain string,
        operatingsystem string,
        systemlanguage string,
        devicebrand string,
        devicetype string,
        browserkernel string,
        respStatus int,
        winPrice double,
        winNotices bigint,
        test INT
)
PARTITIONED BY (l_time string, b_date string, b_time string)
STORED AS ORC;


---- Hive For bigquerry
--drop view ssp_report_overall_dm;
--create view ssp_report_overall_dm as
--select
--    dwr.publisherId   as publisherid,--INT,
--    dwr.appId         as appid,--INT,
--    dwr.countryId     as countryid,--INT,
--    dwr.carrierId     as carrierid,--INT,
--    dwr.adType        as adtype,--INT,
--    dwr.campaignId    as campaignid,--INT,
--    dwr.offerId       as offerid,--INT,
--    dwr.imageId       as imageid,--INT,
--    dwr.affSub        as affsub,-- STRING,
--    dwr.requestCount  as requestcount,-- BIGINT,
--    dwr.sendCount     as sendcount,-- BIGINT,
--    dwr.showCount     as showcount,-- BIGINT,
--    dwr.clickCount    as clickcount,--BIGINT,
--    dwr.feeReportCount as feereportcount,-- BIGINT,         -- 计费条数
--    dwr.feeSendCount   as feesendcount,--BIGINT,         -- 计费显示条数
--    dwr.feeReportPrice as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
--    dwr.feeSendPrice  as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
--    dwr.cpcBidPrice   as cpcbidprice,-- DECIMAL(19,10),
--    dwr.cpmBidPrice   as cpmbidprice,-- DECIMAL(19,10),
--    dwr.conversion    as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
--    dwr.allConversion as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
--    dwr.revenue       as revenue,-- DECIMAL(19,10), -- 收益
--    dwr.realRevenue   as realrevenue,-- DECIMAL(19,10) -- 真实收益
--    dwr.l_time,
--    dwr.b_date,
--
--    p.amId            as publisheramid,
--    p_am.name         as publisheramname,
--    ad.amId           as advertiseramid,
--    a_am.name         as advertiseramname,
--    a.mode            as appmodeid,
--    m.name            as appmodename,
--    cam.adCategory1   as adcategory1id, --INT 关联campaign.adCategory1
--    adc.name          as adcategory1name,
--    cam.name          as campaignname,
--    cam.adverId       as adverid,
--    ad.name           as advername,
--    o.optStatus       as offeroptstatus,
--    o.name            as offername,
--    p.name            as publishername,
--    a.name            as appname,
--    i.iab1name        as iab1name,
--    i.iab2name        as iab2name,
--    c.name            as countryname,
--    ca.name           as carriername,
--    dwr.adType        as adtypeid,   --adFormatId
--    adt.name          as adtypename,
--    v.id              as versionid,
--    dwr.versionName   as versionname,
--    p_am.proxyId      as publisherproxyid,
--    dwr.feeCpcTimes             as feecpctimes,--cpc计费转化数
--    dwr.feeCpmTimes             as feecpmtimes,
--    dwr.feeCpaTimes             as feecpatimes,
--    dwr.feeCpaSendTimes         as feecpasendtimes,--cpc 计费下发数
--    dwr.feeCpcReportPrice       as feecpcreportprice,--cpc 计费上游收益
--    dwr.feeCpmReportPrice       as feecpmreportprice,
--    dwr.feeCpaReportPrice       as feecpareportprice,
--    dwr.feeCpcSendPrice         as feecpcsendprice,
--    dwr.feeCpmSendPrice         as feecpmsendprice,
--    dwr.feeCpaSendPrice         as feecpasendprice,
--    c.alpha2_code               as countrycode,
--
--    dwr.packageName,
--    dwr.domain,
--    dwr.operatingSystem,
--    dwr.systemLanguage,
--    dwr.deviceBrand,
--    dwr.deviceType,
--    dwr.browserKernel,
--    dwr.b_time,
--    dwr.respStatus,
--    dwr.winPrice,
--    dwr.winNotices
--from ssp_report_overall_dwr dwr
--left join campaign cam      on cam.id = dwr.campaignId
--left join advertiser ad     on ad.id = cam.adverId
--left join employee a_am     on a_am.id = ad.amid
--left join offer o           on o.id = dwr.offerId
--left join publisher p       on p.id = dwr.publisherId
--left join employee p_am     on p_am.id = p.amid
--left join app a             on a.id = dwr.appId
--left join iab i             on i.iab1 = a.iab1 and i.iab2 = a.iab2
--left join app_mode m        on m.id = a.mode
--left join country c         on c.id = dwr.countryId
--left join carrier ca        on ca.id = dwr.carrierId
--left join ad_type adt       on adt.id = dwr.adType
--left join version_control v on v.version = dwr.versionName
--left join ad_category1 adc  on adc.id =  cam.adCategory1
--where v.id is not null;



--*******************************************************************************************************
-- Overall for hour DM - START
--*******************************************************************************************************
--hive view for bigquerry
drop view ssp_report_overall_dm;
create view ssp_report_overall_dm as
select
    nvl(dwr.publisherId, a.publisherId) as publisherid, --新增/活跃用户数据没有含publisherId，需通过配置表关联得到
--    dwr.publisherId   as publisherid,--INT,
    dwr.appId         as appid,--INT,
    dwr.countryId     as countryid,--INT,
    dwr.carrierId     as carrierid,--INT,
--    dwr.versionName   ,-- STRING,         -- eg: v1.2.3
    dwr.adType        as adtype,--INT,
    dwr.campaignId    as campaignid,--INT,
    dwr.offerId       as offerid,--INT,
    dwr.imageId       as imageid,--INT,
    dwr.affSub        as affsub,-- STRING,
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
    dwr.b_date,
--    p.amId            as publisheramid,
--    p_am.name         as publisheramname,
    nvl(p.amId, a_p.amId) as publisheramid,
    nvl(p_am.name, ap_am.name) as publisheramname,
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
--    p.name            as publishername,
    nvl(p.name, a_p.name ) as publishername,
    a.name            as appname,
    i.iab1name        as iab1name,
    i.iab2name        as iab2name,
    c.name            as countryname,
    ca.name           as carriername,
    dwr.adType        as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
    dwr.versionName   as versionname,
    p_am.proxyId      as publisherproxyid,
    cast(null as string)              as data_type,
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
    c.alpha2_code           as countrycode,

    dwr.packageName,
    dwr.domain,
    dwr.operatingSystem,
    dwr.systemLanguage,
    dwr.deviceBrand,
    dwr.deviceType,
    dwr.browserKernel,

    dwr.respStatus,
    dwr.winPrice,
    dwr.winNotices,
    a.isSecondHighPriceWin,
    co.id as companyid,
    co.name as companyname,
    dwr.test,
    dwr.ruleId,
    dwr.smartId,
    pro.id as proxyId,
    s.name as smartName,
    sr.name as ruleName,
    co.id as appCompanyid,
    co_o.id as offerCompanyid,
    newCount,                      -- 新增用户数
    activeCount,                   -- 活跃用户数
    cam.adCategory2   as adcategory2id,
    adc2.name as adcategory2name,
    -- 新角色id（渠道 AM id）区分之前DB的 am id
    -- 新增/活跃用户数据没有含publisherId，publisherampaid需通过配置表关联appId -> app -> publisher -> 关联得到
    nvl(p.ampaId, a_p.ampaId) as publisherampaid,
    nvl(p_amp.name, ap_amp.name) as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name           as advertiseramaaname,
    dwr.eventName
from ssp_report_overall_dwr dwr
left join campaign cam      on cam.id = dwr.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join employee a_am     on a_am.id = ad.amid
left join offer o           on o.id = dwr.offerId
left join publisher p       on p.id = dwr.publisherId
left join employee p_am     on p_am.id = p.amid
left join app a             on a.id = dwr.appId
left join iab i             on i.iab1 = a.iab1 and i.iab2 = a.iab2
left join app_mode m        on m.id = a.mode
left join country c         on c.id = dwr.countryId
left join carrier ca        on ca.id = dwr.carrierId
left join ad_type adt       on adt.id = dwr.adType
left join version_control v on v.version = dwr.versionName
left join ad_category1 adc  on adc.id =  cam.adCategory1
left join ad_category2 adc2 on adc2.id =  cam.adCategory2

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId
left join other_smart_link s on s.ID = dwr.smartId
left join smartlink_rules  sr on sr.ID = dwr.ruleId

left join campaign cam_o    on cam_o.id = o.campaignId
left join advertiser ad_o   on ad_o.id = cam_o.adverId
left join employee em_o     on em_o.id = ad_o.amId
left join company co_o      on co_o.id = em_o.companyId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee ap_amp   on  ap_amp.id  = a_p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId

where v.id is not null and b_date > "2018-01-15"  -- b_date > "2018-01-03"

union all
select
    dm.publisherid,--INT,
    dm.appid,--INT,
    dm.countryid,--INT,
    dm.carrierid,--INT,
    dm.adtype,--INT,
    dm.campaignid,--INT,
    dm.offerid,--INT,
    dm.imageid,--INT,
    dm.affsub,-- STRING,
    dm.requestcount,-- BIGINT,
    dm.sendcount,-- BIGINT,
    dm.showcount,-- BIGINT,
    dm.clickcount,--BIGINT,
    dm.feereportcount,-- BIGINT,         -- 计费条数
    dm.feesendcount,--BIGINT,         -- 计费显示条数
    dm.feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
    dm.feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
    dm.cpcbidprice,-- DECIMAL(19,10),
    dm.cpmbidprice,-- DECIMAL(19,10),
    dm.conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    dm.allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
    dm.revenue,-- DECIMAL(19,10), -- 收益
    dm.realrevenue,-- DECIMAL(19,10) -- 真实收益
    concat(b_date, ' 00:00:00') as b_time,
    dm.l_time,
    dm.b_date,

    dm.publisheramid,
    dm.publisheramname,
    dm.advertiseramid,
    dm.advertiseramname,
    dm.appmodeid,
    dm.appmodename,
    dm.adcategory1id, --INT 关联campaign.adCategory1
    dm.adcategory1name,
    dm.campaignname,
    dm.adverid,
    dm.advername,
    dm.offeroptstatus,
    dm.offername,
    dm.publishername,
    dm.appname,
    cast(null as string) as iab1name,
    cast(null as string) as iab2name,
    dm.countryname,
    dm.carriername,
    dm.adtypeid,   --adFormatId
    dm.adtypename,
    dm.versionid,
    dm.versionname,
    dm.publisherproxyid,
    dm.data_type,
    dm.feecpctimes,--cpc计费转化数
    dm.feecpmtimes,
    dm.feecpatimes,
    dm.feecpasendtimes,--cpc 计费下发数
    dm.feecpcreportprice,--cpc 计费上游收益
    dm.feecpmreportprice,
    dm.feecpareportprice,
    dm.feecpcsendprice,
    dm.feecpmsendprice,
    dm.feecpasendprice,
    dm.countrycode,

    cast(null as string)        as packageName,
    cast(null as string)        as domain,
    cast(null as string)        as operatingSystem,
    cast(null as string)        as systemLanguage,
    cast(null as string)        as deviceBrand,
    cast(null as string)        as deviceType,
    cast(null as string)        as browserKernel,

    0           as respStatus,
    0           as winPrice,
    0           as winNotices,
    2           as isSecondHighPriceWin,
    companyid,
    companyname,
    cast(null as int) as test,
    cast(null as int) as ruleId,
    cast(null as int) as smartId,
    cast(null as int) as proxyId,
    cast(null as string) as smartName,
    cast(null as string) as ruleName,
    cast(null as int) as appCompanyid,
    cast(null as int) as offerCompanyid,
    cast(null as bigint) as newCount,
    cast(null as bigint) as activeCount,
    cast(null as int) as adcategory2id,
    cast(null as string) as adcategory2name,
    cast(null as int) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    cast(null as string) as publisherampaname,
    cast(null as int)    as advertiseramaaid,
    cast(null as string) as advertiseramaaname,
    cast(null as string) as eventName
from ssp_report_campaign_dm dm
where b_date <= "2018-01-15";  -- b_date <= "2018-01-03";
--*******************************************************************************************************
-- Overall for hour DM - END
--*******************************************************************************************************

----postback and event
create view ssp_overall_events_dm as
select
    dwi.*,
    cam.name          as campaignname,
    ad.name           as advername,
    o.name            as offername,
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,

    p.ampaId          as ampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name        as ampaname,
    ad.amaaId         as amaaid,
    a_ama.name        as amaaname,
    cam.adverId       as adverid
from ssp_overall_events_dwi dwi
left join campaign cam      on cam.id = dwi.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join offer o           on o.id = dwi.offerId
left join publisher p       on p.id = dwi.publisherId
left join app a             on a.id = dwi.subId
left join country c         on c.id = dwi.countryId
left join carrier ca        on ca.id = dwi.carrierId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId;

----------------------------------
--drop view ssp_overall_postback_dm;
create view ssp_overall_postback_dm as
select
--    dwi.*,
    dwi.repeats,
    dwi.rowkey,
    dwi.id,
    dwi.publisherid,
    dwi.subid,
    dwi.offerid,
    dwi.campaignid,
    dwi.countryid,
    dwi.carrierid,
    dwi.devicetype,
    dwi.useragent,
    dwi.ipaddr,
    dwi.clickid,
    dwi.price,
    dwi.reporttime,
    dwi.createtime,
    dwi.clicktime,
    dwi.showtime,
    dwi.requesttype,
    dwi.pricemethod,
    dwi.bidprice,
    dwi.adtype,
    dwi.issend,
    ROUND(dwi.reportprice,4) as reportprice,
    ROUND(dwi.sendprice,4) as sendprice,
    dwi.s1,
    dwi.s2,
    dwi.gaid,
    dwi.androidid,
    dwi.idfa,
    dwi.postback,
    dwi.sendstatus,
    dwi.sendtime,
    dwi.sv,
    dwi.imei,
    dwi.imsi,
    dwi.imageid,
    dwi.affsub,
    dwi.s3,
    dwi.s4,
    dwi.s5,
    dwi.packagename,
    dwi.domain,
    dwi.respstatus,
    dwi.winprice,
    dwi.wintime,
    dwi.appprice,
    dwi.test,
    dwi.ruleid,
    dwi.smartid,
    dwi.pricepercent,
    dwi.apppercent,
    dwi.salepercent,
    dwi.appsalepercent,
    dwi.reportip,
    dwi.eventname,
    dwi.eventvalue,
    dwi.refer,
    dwi.status,
    dwi.city,
    dwi.region,
    dwi.repeated,
    dwi.l_time,
    dwi.b_date,
    dwi.b_time,

    cam.adverId       as adverid,
    cam.name          as campaignname,
    ad.name           as advername,
    o.name            as offername,
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,

    p.ampaId          as ampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name        as ampaname,
    ad.amaaId         as amaaid,
    a_ama.name        as amaaname

from ssp_overall_postback_dwi dwi
left join campaign cam      on cam.id = dwi.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join offer o           on o.id = dwi.offerId
left join publisher p       on p.id = dwi.publisherId
left join app a             on a.id = dwi.subId
left join country c         on c.id = dwi.countryId
left join carrier ca        on ca.id = dwi.carrierId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId;

-----
CREATE TABLE ad_category2(id int, name varchar(100));
INSERT INTO ad_category2
VALUES
    (101, 'Adultdating'),
    (102, 'Gay'),
    (103, 'Straight'),
    (201, 'Consumergoods'),
    (202, 'Dating'),
    (203, 'Education'),
    (204, 'Entertainment'),
    (205, 'Finance'),
    (206, 'Gambling/casino'),
    (207, 'Games'),
    (208, 'Leadgeneration'),
    (209, 'Mobilecontentsubscription'),
    (210, 'Mobilesoftware'),
    (211, 'Shopping retail'),
    (212, 'Social'),
    (213, 'Sweepstakes'),
    (214, 'Travel'),
    (215, 'Utilities tools'),
    (216, 'GP'),
    (217, 'Smart contents'),
    (218, 'Nutra'),
    (219, 'Video'),
    (220, 'Music'),
    (221, 'Antivirus'),
    (222, 'DDL');


select count(1) from campaign  group by id order by 1 desc limit 1;select count(1) from advertiser group by id order by 1 desc limit 1;select count(1) from employee group by id order by 1 desc limit 1;select count(1) from campaign  group by id order by 1 desc limit 1;select count(1) from offer group by id order by 1 desc limit 1;select count(1) from publisher group by id order by 1 desc limit 1;select count(1) from publisher group by id order by 1 desc limit 1;select count(1) from app group by id order by 1 desc limit 1;select count(1) from iab group by iab1, iab2 order by 1 desc limit 1;select count(1) from app_mode group by id order by 1 desc limit 1;select count(1) from country group by id order by 1 desc limit 1;select count(1) from carrier group by id order by 1 desc limit 1;select count(1) from ad_type group by id order by 1 desc limit 1;select count(1) from version_control group by version order by 1 desc limit 1;select count(1) from ad_category1 group by id order by 1 desc limit 1;







------------------------------------------------------------------------------------------------------------------------
-- 离线统计(spark beeline方式不支持add jar)
------------------------------------------------------------------------------------------------------------------------
-- add jar file:///root/kairenlo/data-streaming/ssp/overall/data-streaming.jar;
CREATE FUNCTION browserKernel AS 'com.mobikok.ssp.data.streaming.udf.UserAgentBrowserKernelUDF';
CREATE FUNCTION deviceType AS 'com.mobikok.ssp.data.streaming.udf.UserAgentDeviceTypeUDF';
CREATE FUNCTION operatingSystem AS 'com.mobikok.ssp.data.streaming.udf.UserAgentOperatingSystemUDF';
CREATE FUNCTION language AS 'com.mobikok.ssp.data.streaming.udf.UserAgentLanguageUDF';
CREATE FUNCTION machineModel AS 'com.mobikok.ssp.data.streaming.udf.UserAgentMachineModelUDF';




drop view if exists ssp_report_overall_dwr_view;
create view ssp_report_overall_dwr_view as
select
    publisherId,
    appId,
    countryId,
    carrierId,
    versionName,
    adType,
    campaignId,
    offerId,
    imageId,
    affSub,
    sum(requestCount)              as requestCount,
    sum(sendCount)                 as sendCount,
    sum(showCount)                 as showCount,
    sum(clickCount)                as clickCount,
    sum(feeReportCount)            as feeReportCount,    -- 计费条数
    sum(feeSendCount)              as feeSendCount,      -- 计费显示条数
    sum(feeReportPrice)            as feeReportPrice,    -- 计费金额(真实收益)
    sum(feeSendPrice)              as feeSendPrice,      -- 计费显示金额(收益)
    sum(cpcBidPrice)               as cpcBidPrice,
    sum(cpmBidPrice)               as cpmBidPrice,
    sum(conversion)                as conversion,        -- 转化数，目前不要含展示和点击产生的
    sum(allConversion)             as allConversion,     -- 转化数，含展示和点击产生的
    sum(revenue)                   as revenue,           -- 收益
    sum(realRevenue)               as realRevenue,       -- 真实收益
    sum(feeCpcTimes)               as feeCpcTimes,       -- fee cpc转化条数
    sum(feeCpmTimes)               as feeCpmTimes,       -- fee cpm转化条数
    sum(feeCpaTimes)               as feeCpaTimes,       -- fee cpa转化条数
    sum(feeCpaSendTimes)           as feeCpaSendTimes,   -- fee cpa send转化条数
    sum(feeCpcReportPrice)         as feeCpcReportPrice, -- fee cpc上游收益
    sum(feeCpmReportPrice)         as feeCpmReportPrice, -- fee cpm上游收益
    sum(feeCpaReportPrice)         as feeCpaReportPrice, -- fee cpa上游收益
    sum(feeCpcSendPrice)           as feeCpcSendPrice,   -- fee cpc下游收益
    sum(feeCpmSendPrice)           as feeCpmSendPrice,   -- fee cpm下游收益
    sum(feeCpaSendPrice)           as feeCpaSendPrice,   -- fee cpa下游收益
    packageName,
    domain,
    operatingSystem,
    systemLanguage,
    deviceBrand,
    deviceType,
    browserKernel,
    respStatus,
    sum(winPrice)                  as winPrice,          -- 中签价格
    sum(winNotices)                as winNotices,        -- 中签数
    test,
    ruleId,
    smartId,
    sum(newCount)                  as newCount,
    sum(activeCount)               as activeCount,

    l_time,
    b_date,
    b_time
from (
    select
        publisherId,
        subId                      as appId,
        countryId,
        carrierId,
        sv                         as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent) as operatingSystem,
        language(userAgent)        as systemLanguage,
        machineModel(userAgent)    as deviceBrand,
        deviceType(userAgent)      as deviceType,
        browserKernel(userAgent)   as browserKernel,
        respStatus                 as respStatus,
        test                       as test,
        ruleId                     as ruleId,
        smartId                    as smartId,
        1                          as requestCount,
        0                          as sendCount,
        0                          as showCount,
        0                          as clickCount,
        0                          as feeReportCount,    -- 计费条数
        0                          as feeSendCount,      -- 计费显示条数
        0                          as feeReportPrice,    -- 计费金额(真实收益)
        0                          as feeSendPrice,      -- 计费显示金额(收益)
        0                          as cpcBidPrice,
        0                          as cpmBidPrice,
        0                          as conversion,        -- 转化数，目前不要含展示和点击产生的
        0                          as allConversion,     -- 转化数，含展示和点击产生的
        0                          as revenue,           -- 收益
        0                          as realRevenue,       -- 真实收益
        0                          as feeCpcTimes,       -- fee cpc转化条数
        0                          as feeCpmTimes,       -- fee cpm转化条数
        0                          as feeCpaTimes,       -- fee cpa转化条数
        0                          as feeCpaSendTimes,   -- fee cpa send转化条数
        0                          as feeCpcReportPrice, -- fee cpc上游收益
        0                          as feeCpmReportPrice, -- fee cpm上游收益
        0                          as feeCpaReportPrice, -- fee cpa上游收益
        0                          as feeCpcSendPrice,   -- fee cpc下游收益
        0                          as feeCpmSendPrice,   -- fee cpm下游收益
        0                          as feeCpaSendPrice,   -- fee cpa下游收益
        0                          as winPrice,          -- 中签价格
        0                          as winNotices,        -- 中签数
        0                          as newCount,
        0                          as activeCount,

--      var dwiBTimeFormat = "yyyy-MM-dd 00:00:00"
--      var dwrBTimeFormat = "yyyy-MM-dd HH:00:00"
--      var dwiLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd HH:00:00")
--      var dwrLoadTimeFormat = CSTTime.formatter("yyyy-MM-dd 00:00:00")
        date_format(l_time, 'yyyy-MM-dd 00:00:ss')     as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
--        date_format(createTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_fill_dwi
    union all
    select
        publisherId,
        subId                      as appId,
        countryId,
        carrierId,
        sv                         as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent) as operatingSystem,
        language(userAgent)        as systemLanguage,
        machineModel(userAgent)    as deviceBrand,
        deviceType(userAgent)      as deviceType,
        browserKernel(userAgent)   as browserKernel,
        respStatus                 as respStatus,
        test                       as test,
        ruleId                     as ruleId,
        smartId                    as smartId,
        0                          as requestCount,
        1                          as sendCount,
        0                          as showCount,
        0                          as clickCount,
        0                          as feeReportCount,    -- 计费条数
        0                          as feeSendCount,      -- 计费显示条数
        0                          as feeReportPrice,    -- 计费金额(真实收益)
        0                          as feeSendPrice,      -- 计费显示金额(收益)
        0                          as cpcBidPrice,
        0                          as cpmBidPrice,
        0                          as conversion,        -- 转化数，目前不要含展示和点击产生的
        0                          as allConversion,     -- 转化数，含展示和点击产生的
        0                          as revenue,           -- 收益
        0                          as realRevenue,       -- 真实收益
        0                          as feeCpcTimes,       -- fee cpc转化条数
        0                          as feeCpmTimes,       -- fee cpm转化条数
        0                          as feeCpaTimes,       -- fee cpa转化条数
        0                          as feeCpaSendTimes,   -- fee cpa send转化条数
        0                          as feeCpcReportPrice, -- fee cpc上游收益
        0                          as feeCpmReportPrice, -- fee cpm上游收益
        0                          as feeCpaReportPrice, -- fee cpa上游收益
        0                          as feeCpcSendPrice,   -- fee cpc下游收益
        0                          as feeCpmSendPrice,   -- fee cpm下游收益
        0                          as feeCpaSendPrice,   -- fee cpa下游收益
        0                          as winPrice,          -- 中签价格
        0                          as winNotices,        -- 中签数
        0                          as newCount,
        0                          as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')     as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
        --date_format(createTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_send_dwi
    union all
    select
        publisherId,
        subId                      as appId,
        countryId,
        carrierId,
        sv                         as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent) as operatingSystem,
        language(userAgent)        as systemLanguage,
        machineModel(userAgent)    as deviceBrand,
        deviceType(userAgent)      as deviceType,
        browserKernel(userAgent)   as browserKernel,
        respStatus                 as respStatus,
        test                       as test,
        ruleId                     as ruleId,
        smartId                    as smartId,
        0                          as requestCount,
        0                          as sendCount,
        1                          as showCount,
        0                          as clickCount,
        0                          as feeReportCount,    -- 计费条数
        0                          as feeSendCount,      -- 计费显示条数
        0                          as feeReportPrice,    -- 计费金额(真实收益)
        0                          as feeSendPrice,      -- 计费显示金额(收益)
        0                          as cpcBidPrice,
        cast( if( priceMethod = 2,  bidPrice, 0) as decimal(19,10) )/1000 as cpmBidPrice,
        0                                                                 as conversion,        -- 转化数，目前不要含展示和点击产生的
        if( priceMethod = 2, 1, null)                                     as allConversion,     -- 转化数，含展示和点击产生的
        cast( if( priceMethod = 2, sendPrice, 0) as decimal(19,10) )      as revenue,           -- 收益
        cast( if( priceMethod = 2, bidPrice, 0) as decimal(19,10) )/1000  as realRevenue,       -- 真实收益
        0                          as feeCpcTimes,       -- fee cpc转化条数
        0                          as feeCpmTimes,       -- fee cpm转化条数
        0                          as feeCpaTimes,       -- fee cpa转化条数
        0                          as feeCpaSendTimes,   -- fee cpa send转化条数
        0                          as feeCpcReportPrice, -- fee cpc上游收益
        0                          as feeCpmReportPrice, -- fee cpm上游收益
        0                          as feeCpaReportPrice, -- fee cpa上游收益
        0                          as feeCpcSendPrice,   -- fee cpc下游收益
        0                          as feeCpmSendPrice,   -- fee cpm下游收益
        0                          as feeCpaSendPrice,   -- fee cpa下游收益
        0                          as winPrice,          -- 中签价格
        0                          as winNotices,        -- 中签数
        0                          as newCount,
        0                          as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')   as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
        --date_format(showTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_show_dwi
    union all
    select
        publisherId,
        subId                      as appId,
        countryId,
        carrierId,
        sv                         as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent) as operatingSystem,
        language(userAgent)        as systemLanguage,
        machineModel(userAgent)    as deviceBrand,
        deviceType(userAgent)      as deviceType,
        browserKernel(userAgent)   as browserKernel,
        respStatus                 as respStatus,
        test                       as test,
        ruleId                     as ruleId,
        smartId                    as smartId,
        0                          as requestCount,
        0                          as sendCount,
        0                          as showCount,
        1                          as clickCount,
        0                          as feeReportCount,    -- 计费条数
        0                          as feeSendCount,      -- 计费显示条数
        0                          as feeReportPrice,    -- 计费金额(真实收益)
        0                          as feeSendPrice,      -- 计费显示金额(收益)
        cast( if( priceMethod = 1,  bidPrice, 0) as decimal(19,10) )                          as cpcBidPrice,
        0                          as cpmBidPrice,
        0                          as conversion,        -- 转化数，目前不要含展示和点击产生的
        if( priceMethod = 1,  1, null)                          as allConversion,     -- 转化数，含展示和点击产生的
        cast( if( priceMethod = 1,  sendPrice, 0) as decimal(19,10) )                         as revenue,           -- 收益
        cast( if( priceMethod = 1,  bidPrice, 0) as decimal(19,10) )                          as realRevenue,       -- 真实收益
        0                          as feeCpcTimes,       -- fee cpc转化条数
        0                          as feeCpmTimes,       -- fee cpm转化条数
        0                          as feeCpaTimes,       -- fee cpa转化条数
        0                          as feeCpaSendTimes,   -- fee cpa send转化条数
        0                          as feeCpcReportPrice, -- fee cpc上游收益
        0                          as feeCpmReportPrice, -- fee cpm上游收益
        0                          as feeCpaReportPrice, -- fee cpa上游收益
        0                          as feeCpcSendPrice,   -- fee cpc下游收益
        0                          as feeCpmSendPrice,   -- fee cpm下游收益
        0                          as feeCpaSendPrice,   -- fee cpa下游收益
        0                          as winPrice,          -- 中签价格
        0                          as winNotices,        -- 中签数
        0                          as newCount,
        0                          as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')    as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
        --date_format(clickTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_click_dwi
    union all
    select
        publisherId,
        subId                                                                        as appId,
        countryId,
        carrierId,
        sv                                                                           as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent)                                                   as operatingSystem,
        language(userAgent)                                                          as systemLanguage,
        machineModel(userAgent)                                                      as deviceBrand,
        deviceType(userAgent)                                                        as deviceType,
        browserKernel(userAgent)                                                     as browserKernel,
        respStatus                                                                   as respStatus,
        test                                                                         as test,
        ruleId                                                                       as ruleId,
        smartId                                                                      as smartId,
        0                                                                            as requestCount,
        0                                                                            as sendCount,
        0                                                                            as showCount,
        0                                                                            as clickCount,
        1                                                                            as feeReportCount,    -- 计费条数
        if(isSend = 1, 1, null)                                                      as feeSendCount,      -- 计费显示条数
        cast(reportPrice as decimal(19,10))                                          as feeReportPrice,    -- 计费金额(真实收益)
        cast(sendPrice as decimal(19,10))                                            as feeSendPrice,      -- 计费显示金额(收益)
        0                                                                            as cpcBidPrice,
        0                                                                            as cpmBidPrice,
        1                                                                            as conversion,        -- 转化数，目前不要含展示和点击产生的
        1                                                                            as allConversion,     -- 转化数，含展示和点击产生的
        cast(sendPrice as decimal(19,10))                                            as revenue,           -- 收益
        cast(reportPrice as decimal(19,10))                                          as realRevenue,       -- 真实收益
        if(priceMethod = 1,  1, null)                                                as feeCpcTimes,       -- fee cpc转化条数
        if(priceMethod = 2,  1, null)                                                as feeCpmTimes,       -- fee cpm转化条数
        if(priceMethod = 3,  1, null)                                                as feeCpaTimes,       -- fee cpa转化条数
        if( priceMethod = 3 and isSend = 1,  1, null)                                as feeCpaSendTimes,   -- fee cpa send转化条数
        cast( if( priceMethod = 1,  reportPrice, 0) as decimal(19,10) )              as feeCpcReportPrice, -- fee cpc上游收益
        cast( if( priceMethod = 2,  reportPrice, 0) as decimal(19,10) )              as feeCpmReportPrice, -- fee cpm上游收益
        cast( if( priceMethod = 3,  reportPrice, 0) as decimal(19,10) )              as feeCpaReportPrice, -- fee cpa上游收益
        cast( if( priceMethod = 1,  sendPrice, 0) as decimal(19,10) )                as feeCpcSendPrice,   -- fee cpc下游收益
        cast( if( priceMethod = 2,  sendPrice, 0) as decimal(19,10) )                as feeCpmSendPrice,   -- fee cpm下游收益
        cast( if( priceMethod = 3 and isSend = 1,  sendPrice, 0) as decimal(19,10) ) as feeCpaSendPrice,   -- fee cpa下游收益
        0                                                                            as winPrice,          -- 中签价格
        0                                                                            as winNotices,        -- 中签数
        0                                                                            as newCount,
        0                                                                            as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')     as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
        --date_format(reportTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_fee_dwi
    union all
    select
        publisherId,
        subId                            as appId,
        countryId,
        carrierId,
        sv                               as versionName,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        packageName,
        domain,
        operatingSystem(userAgent)       as operatingSystem,
        language(userAgent)              as systemLanguage,
        machineModel(userAgent)          as deviceBrand,
        deviceType(userAgent)            as deviceType,
        browserKernel(userAgent)         as browserKernel,
        respStatus                       as respStatus,
        test                             as test,
        ruleId                           as ruleId,
        smartId                          as smartId,
        0                                as requestCount,
        0                                as sendCount,
        0                                as showCount,
        0                                as clickCount,
        0                                as feeReportCount,    -- 计费条数
        0                                as feeSendCount,      -- 计费显示条数
        0                                as feeReportPrice,    -- 计费金额(真实收益)
        0                                as feeSendPrice,      -- 计费显示金额(收益)
        0                                as cpcBidPrice,
        0                                as cpmBidPrice,
        0                                as conversion,        -- 转化数，目前不要含展示和点击产生的
        0                                as allConversion,     -- 转化数，含展示和点击产生的
        0                                as revenue,           -- 收益
        0                                as realRevenue,       -- 真实收益
        0                                as feeCpcTimes,       -- fee cpc转化条数
        0                                as feeCpmTimes,       -- fee cpm转化条数
        0                                as feeCpaTimes,       -- fee cpa转化条数
        0                                as feeCpaSendTimes,   -- fee cpa send转化条数
        0                                as feeCpcReportPrice, -- fee cpc上游收益
        0                                as feeCpmReportPrice, -- fee cpm上游收益
        0                                as feeCpaReportPrice, -- fee cpa上游收益
        0                                as feeCpcSendPrice,   -- fee cpc下游收益
        0                                as feeCpmSendPrice,   -- fee cpm下游收益
        0                                as feeCpaSendPrice,   -- fee cpa下游收益
        cast(winPrice as decimal(19,10)) as winPrice,          -- 中签价格
        1                                as winNotices,        -- 中签数
        0                                as newCount,
        0                                as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')  as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
        --date_format(winTime, 'yyyy-MM-dd 00:00:ss') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_overall_win_dwi
    union all
    select
        null                             as publisherId,
        appId                            as appId,
        countryId                        as countryId,
        carrierId                        as carrierId,
        sv                               as versionName,
        null                             as adType,
        null                             as campaignId,
        null                             as offerId,
        null                             as imageId,
        affSub                           as affSub,
        null                             as packageName,
        null                             as domain,
        operatingSystem(userAgent)       as operatingSystem,
        language(userAgent)              as systemLanguage,
        machineModel(userAgent)          as deviceBrand,
        deviceType(userAgent)            as deviceType,
        browserKernel(userAgent)         as browserKernel,
        null                             as respStatus,
        null                             as test,
        null                             as ruleId,
        null                             as smartId,
        0                                as requestCount,
        0                                as sendCount,
        0                                as showCount,
        0                                as clickCount,
        0                                as feeReportCount,    -- 计费条数
        0                                as feeSendCount,      -- 计费显示条数
        0                                as feeReportPrice,    -- 计费金额(真实收益)
        0                                as feeSendPrice,      -- 计费显示金额(收益)
        0                                as cpcBidPrice,
        0                                as cpmBidPrice,
        0                                as conversion,        -- 转化数，目前不要含展示和点击产生的
        0                                as allConversion,     -- 转化数，含展示和点击产生的
        0                                as revenue,           -- 收益
        0                                as realRevenue,       -- 真实收益
        0                                as feeCpcTimes,       -- fee cpc转化条数
        0                                as feeCpmTimes,       -- fee cpm转化条数
        0                                as feeCpaTimes,       -- fee cpa转化条数
        0                                as feeCpaSendTimes,   -- fee cpa send转化条数
        0                                as feeCpcReportPrice, -- fee cpc上游收益
        0                                as feeCpmReportPrice, -- fee cpm上游收益
        0                                as feeCpaReportPrice, -- fee cpa上游收益
        0                                as feeCpcSendPrice,   -- fee cpc下游收益
        0                                as feeCpmSendPrice,   -- fee cpm下游收益
        0                                as feeCpaSendPrice,   -- fee cpa下游收益
        0                                as winPrice,          -- 中签价格
        0                                as winNotices,        -- 中签数
        1                                as newCount,
        0                                as activeCount,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss')     as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        date_format(createTime, 'yyyy-MM-dd HH:00:00') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_user_new_dwi
    union all
    select
        null                             as publisherId,
        appId                            as appId,
        countryId                        as countryId,
        carrierId                        as carrierId,
        sv                               as versionName,
        null                             as adType,
        null                             as campaignId,
        null                             as offerId,
        null                             as imageId,
        affSub                           as affSub,
        null                             as packageName,
        null                             as domain,
        operatingSystem(userAgent)       as operatingSystem,
        language(userAgent)              as systemLanguage,
        machineModel(userAgent)          as deviceBrand,
        deviceType(userAgent)            as deviceType,
        browserKernel(userAgent)         as browserKernel,
        null                             as respStatus,
        null                             as test,
        null                             as ruleId,
        null                             as smartId,
        0                                as requestCount,
        0                                as sendCount,
        0                                as showCount,
        0                                as clickCount,
        0                                as feeReportCount,    -- 计费条数
        0                                as feeSendCount,      -- 计费显示条数
        0                                as feeReportPrice,    -- 计费金额(真实收益)
        0                                as feeSendPrice,      -- 计费显示金额(收益)
        0                                as cpcBidPrice,
        0                                as cpmBidPrice,
        0                                as conversion,        -- 转化数，目前不要含展示和点击产生的
        0                                as allConversion,     -- 转化数，含展示和点击产生的
        0                                as revenue,           -- 收益
        0                                as realRevenue,       -- 真实收益
        0                                as feeCpcTimes,       -- fee cpc转化条数
        0                                as feeCpmTimes,       -- fee cpm转化条数
        0                                as feeCpaTimes,       -- fee cpa转化条数
        0                                as feeCpaSendTimes,   -- fee cpa send转化条数
        0                                as feeCpcReportPrice, -- fee cpc上游收益
        0                                as feeCpmReportPrice, -- fee cpm上游收益
        0                                as feeCpaReportPrice, -- fee cpa上游收益
        0                                as feeCpcSendPrice,   -- fee cpc下游收益
        0                                as feeCpmSendPrice,   -- fee cpm下游收益
        0                                as feeCpaSendPrice,   -- fee cpa下游收益
        0                                as winPrice,          -- 中签价格
        0                                as winNotices,        -- 中签数
        0                                as newCount,
        1                                as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss')     as l_time, -- DWI yyyy-MM-dd HH:00:00 -> DWR yyyy-MM-dd 00:00:00
        b_date,
        date_format(createTime, 'yyyy-MM-dd HH:00:00') as b_time  -- DWI yyyy-MM-dd HH:mm:ss (business.time.extract.by) -> DWR yyyy-MM-dd HH:00:00
    from ssp_user_active_dwi
)t0
group by
    publisherId,
    appId,
    countryId,
    carrierId,
    versionName,
    adType,
    campaignId,
    offerId,
    imageId,
    affSub,
    packageName,
    domain,
    operatingSystem,
    systemLanguage,
    deviceBrand,
    deviceType,
    browserKernel,
    respStatus,
    test,
    ruleId,
    smartId,

    b_time,
    l_time,
    b_date;








--CREATE TABLE IF NOT EXISTS ssp_report_campaign_dwr_tmp like ssp_report_campaign_dwr;
alter table ssp_report_overall_dwr drop partition (b_time = "2018-03-29 08:00:00")

set hive.exec.dynamic.partition.mode=nonstrict;
set spark.default.parallelism = 1;
set spark.sql.shuffle.partitions = 1;

insert overwrite table ssp_report_overall_dwr
select * from ssp_report_overall_dwr_view
where b_time in ("2018-03-29 08:00:00","2018-03-29 02:00:00") and b_date = "2018-03-29"
--where l_time <'2017-09-10';






insert into table ssp_fee_dwi
select
repeats,
rowkey ,
id ,
publisherid,
subid   ,
offerid  ,
campaignid,
countryid  ,
carrierid ,
devicetype ,
useragent ,
ipaddr     ,
clickid   ,
price      ,
reporttime,
createtime ,
clicktime ,
 showtime   ,
 requesttype,
 pricemethod ,
 bidprice   ,
 adtype      ,
 issend     ,
 reportprice ,
 sendprice  ,
 s1          ,
 s2         ,
 gaid        ,
 androidid  ,
 idfa        ,
 postback   ,
 sendstatus  ,
 sendtime   ,
 sv          ,
 imei       ,
 imsi        ,
 imageid    ,
 affsub      ,
 s3         ,
 s4          ,
 s5         ,
 packagename ,
 domain     ,
 reportip    ,
 repeated   ,
 l_time     ,
 b_date
 from ssp_overall_fee_dwi
 where b_time in ("2018-03-29 10:00:00")



insert overwrite table ssp_fee_dwi
select * from ssp_fee_dwi
where  date_format(reportTime, 'yyyy-MM-dd HH:00:00') not in ("2018-03-29 10:00:00")










select count(1)
from ssp_overall_win_dwi
where b_time="2018-03-29 08:00:00"


SELECT b_time,l_time,
sum(requestCount)              as requestCount,
    sum(sendCount)                 as sendCount,
    sum(showCount)                 as showCount,
    sum(clickCount)                as clickCount,
    sum(feeReportCount)            as feeReportCount,    -- 计费条数
    sum(feeSendCount)              as feeSendCount,      -- 计费显示条数
    sum(feeReportPrice)            as feeReportPrice,    -- 计费金额(真实收益)
    sum(feeSendPrice)              as feeSendPrice,      -- 计费显示金额(收益)
    sum(cpcBidPrice)               as cpcBidPrice,
    sum(cpmBidPrice)               as cpmBidPrice,
    sum(conversion)                as conversion,        -- 转化数，目前不要含展示和点击产生的
    sum(allConversion)             as allConversion,     -- 转化数，含展示和点击产生的
    sum(revenue)                   as revenue,           -- 收益
    sum(realRevenue)               as realRevenue,       -- 真实收益
    sum(feeCpcTimes)               as feeCpcTimes,       -- fee cpc转化条数
    sum(feeCpmTimes)               as feeCpmTimes,       -- fee cpm转化条数
    sum(feeCpaTimes)               as feeCpaTimes,       -- fee cpa转化条数
    sum(feeCpaSendTimes)           as feeCpaSendTimes,   -- fee cpa send转化条数
    sum(feeCpcReportPrice)         as feeCpcReportPrice, -- fee cpc上游收益
    sum(feeCpmReportPrice)         as feeCpmReportPrice, -- fee cpm上游收益
    sum(feeCpaReportPrice)         as feeCpaReportPrice, -- fee cpa上游收益
    sum(feeCpcSendPrice)           as feeCpcSendPrice,   -- fee cpc下游收益
    sum(feeCpmSendPrice)           as feeCpmSendPrice,   -- fee cpm下游收益
    sum(feeCpaSendPrice)           as feeCpaSendPrice,   -- fee cpa下游收益
    sum(winPrice)                  as winPrice,          -- 中签价格
    sum(winNotices)                as winNotices,        -- 中签数
    sum(newCount)                  as newCount,
    sum(activeCount)               as activeCount
FROM ssp_report_overall_dwr
WHERE b_time IN ("2018-03-29 08:00:00","2018-03-29 02:00:00", "2018-03-29 03:00:00") and b_date = "2018-03-29"
group by b_time, l_time;


select b_date, sum(conversion) ,sum(feeReportPrice) from ssp_report_overall_dwr__original where b_date >= "2018-06-26" group by b_date order by b_date
2018-06-26      46905   17697.419773587
2018-06-27      59878   21468.2677964103
2018-06-28      45909   18321.1077892726 |||start

select b_date, sum(conversion) ,sum(feeReportPrice) from s__ssp_report_overall_dwr group by b_date order by b_date
2018-06-26      24284   9200.6952527003
2018-06-27      59878   21468.2677964103
2018-06-28      59636   22161.2295496042 ||||jie
2018-06-29      56171   20240.9807212395 ||||end
2018-06-30      59160   18444.8225470893
2018-07-01      41273   12692.8751276179

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition.mode=nonstrict;
alter table ssp_report_overall_dwr__original DROP  partition (b_date="2018-06-27");
alter table ssp_report_overall_dwr__original DROP  partition (b_date="2018-06-28");
insert overwrite table ssp_report_overall_dwr__original
select * from s__ssp_report_overall_dwr where b_date in("2018-06-27", "2018-06-28", "2018-06-29")

insert overwrite table ssp_report_overall_dwr__original
select * from ssp_report_overall_dwr where b_date = "2018-06-30"

insert overwrite table ssp_report_overall_dwr__original
select * from ssp_report_overall_dwr where b_date = "2018-07-02"


alter table ssp_report_overall_dwr rename to ssp_report_overall_dwr__xianfeng;

alter table ssp_report_overall_dwr__original rename to ssp_report_overall_dwr;

alter table ssp_report_overall_dwr_day_base DROP partition (b_date="2018-06-29");


 SELECT
        b_date,
        sum(revenue)  as revenue,
        sum(realrevenue) as realrevenue
        FROM ssp_report_overall_dwr_day
        where b_date >= "2018-06-24" and b_date <="2018-06-32"
        group by b_date
        order by b_date
        ;
2018-06-24      9207.585615581  15367.1317258448
2018-06-25      12158.4616002381        20320.5842961646
2018-06-26      11159.5195703606        17720.130973587

2018-06-27      13505.6412243792        21485.0459964103
2018-06-28      11072.7307143933        18333.5659892726  *
2018-06-29      2915.900622449          4686.8196793566   *

2018-06-30      10678.5095359438        18539.5228470893



2018-07-02	40063	13368.2551
2	2018-07-01	53193	16400.0271
3	2018-06-30	59160	18539.52285
4	2018-06-29	13726	4686.81968
5	2018-06-28	45909	18333.56599
6	2018-06-27	59878	21485.046
7	2018-06-26	46905	17720.13097
8	2018-06-25	51170	20320.5843
9	2018-06-24	38702	15367.13173
10	2018-06-23	37202	15093.43465
11	2018-06-22	46078	17345.62763
12	2018-06-21	49529	18757.02963
13	2018-06-20	47518	19837.83877


27 【天：28 29， 小时：26 27 28 29】 重刷
select b_date, sum(revenue) ,sum(realRevenue) from ssp_report_overall_dwr where b_date >="2018-06-24" group by b_date order by b_date
2018-06-24      9207.585615581          15367.1317258448
2018-06-25      12158.4616002381        20320.5842961646

2018-06-26      11159.5195703606        17720.130973587

2018-06-27      13505.6412243792        21485.0459964103
2018-06-28      13532.8797321471        22318.6218496042
2018-06-29      12215.1746158074        20381.1289212395

2018-06-30      10678.5095359438        18539.5228470893
2018-07-01      9489.7359430109         16400.0271037525
2018-07-02      5727.4153036484         10317.0688815041


select b_date, sum(conversion) ,sum(feeReportPrice) from ssp_report_overall_dwr__original where b_date >="2018-06-27" group by b_date order by b_date
2018-06-27      59878   21468.2677964103
2018-06-28      59636   22161.2295496042
2018-06-29      56171   20240.9807212395


select b_date, sum(conversion) ,sum(feeSendCount) from s__ssp_report_overall_dwr group by b_date order by b_date
2018-06-26      24284   18625
2018-06-27      59878   47229
2018-06-28      59636   44084
2018-06-29      56171   40790
2018-06-30      59160   42264
2018-07-01      41273   28698


select b_date, sum(conversion) ,sum(allconversion) from s__ssp_report_overall_dwr group by b_date order by b_date
2018-06-26      24284   24284
2018-06-27      59878   59899
2018-06-28      59636   402131
2018-06-29      56171   168580
2018-06-30      59160   212895
2018-07-01      41273   41273



select b_date, sum(revenue) ,sum(realRevenue) from s__ssp_report_overall_dwr group by b_date order by b_date
2018-06-26      5835.4023067498         9200.6952527003
2018-06-27      13492.6318533792        21468.2700964103
2018-06-28      13532.8797321471        22318.6218496042
2018-06-29      12148.4935148074        20295.8218212395
2018-06-30      10628.8079239438        18474.6182470893
2018-07-01      7174.6701010383         12692.8751276179

alter table ssp_report_overall_dwr drop partition (b_date = '2018-06-29');
select b_date, sum(conversion) ,sum(feeReportPrice) from ssp_report_overall_dwr group by b_date order by b_date
2018-06-30      59160   18444.8225470893
2018-07-01      53193   16336.7328037525
2018-07-02      26703   8882.8325424605

select b_date, sum(conversion) ,sum(feeReportPrice) from ssp_report_overall_dwr group by b_date order by b_date
2018-06-29      13726   4682.0210793566
2018-06-30      59160   18444.8225470893  |||| start
2018-07-01      50386   15383.3801417893

select b_date, sum(conversion) ,sum(feeSendCount) from ssp_report_overall_dwr group by b_date order by b_date
2018-06-29      13726   10959
2018-06-30      59160   42264
2018-07-01      50386   35592
select b_date, sum(conversion) ,sum(allconversion) from ssp_report_overall_dwr group by b_date order by b_date
2018-06-29      13726   23786
2018-06-30      59160   562889
2018-07-01      50220   479458

select b_date, sum(revenue) ,sum(realRevenue) from ssp_report_overall_dwr group by b_date order by b_date
2018-06-29      2915.756992449          4686.6326793566
2018-06-30      10678.5095359438        18539.5228470893
2018-07-01      8748.2799017777         15231.4481769643



sum(realRevenue)  722609.4550244947
sum(revenue)      541851.5228113012


+--------------------+----------------------+--+
|  sum(realRevenue)  |     sum(revenue)     |
+--------------------+----------------------+--+
| 645119.0000000000  | 23001803.0000000000  |
+--------------------+----------------------+--+


   cast( as decimal(19,10))                                            as ,           -- 收益
        cast(reportPrice as decimal(19,10))

select sum(sendPrice) as realRevenue,  sum(reportPrice) as revenue
from ssp_overall_fee_dwi where b_time IN ("2018-03-29 08:00:00","2018-03-29 02:00:00")





select count(1),date_format(reportTime, 'yyyy-MM-dd HH:00:00')
from ssp_fee_dwi where b_date = "2018-03-29"
group by  date_format(reportTime, 'yyyy-MM-dd HH:00:00')
order by 2



select count(1),date_format(reportTime, 'yyyy-MM-dd HH:00:00')
from ssp_overall_fee_dwi where b_date = "2018-03-29"
group by  date_format(reportTime, 'yyyy-MM-dd HH:00:00')
order by 2




set hive.exec.dynamic.partition.mode=nonstrict;
set spark.default.parallelism = 1;
set   spark.sql.shuffle.partitions = 1;

insert overwrite table ssp_fee_dwi
select * from ssp_fee_dwi
where  date_format(reportTime, 'yyyy-MM-dd HH:00:00') <> "2018-03-29 10:00:00" and b_date = "2018-03-29"

select count(1) ,date_format(reportTime, 'yyyy-MM-dd HH:00:00') as b_time
from ssp_fee_dwi
where b_date = "2018-03-29"
group by date_format(reportTime, 'yyyy-MM-dd HH:00:00')
order by 2

select count(1) ,  b_time
from ssp_overall_fee_dwi
where b_date = "2018-03-29"
group by b_time
order by 2