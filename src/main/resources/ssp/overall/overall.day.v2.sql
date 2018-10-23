--select count(1) from ssp_report_overall_dm_day_v2 where b_date = "2018-06-11"

--*******************************************************************************************************
-- Overall for day DM - START
--*******************************************************************************************************
--hive view for bigquerry
drop view if exists ssp_report_overall_dm_day_v2;
create view ssp_report_overall_dm_day_v2 as
select
    coalesce(dwr.publisherId, a.publisherId) as publisherid, --新增/活跃用户数据没有含publisherId，需通过配置表关联得到
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
    coalesce(p.amId, a_p.amId) as publisheramid,
    coalesce(p_am.name, ap_am.name) as publisheramname,
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
    coalesce(p.name, a_p.name ) as publishername,
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

--    dwr.packageName,
--    dwr.domain,
--    dwr.operatingSystem,
--    dwr.systemLanguage,
--    dwr.deviceBrand,
--    dwr.deviceType,
--    dwr.browserKernel,

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
    coalesce(p.ampaId, a_p.ampaId) as publisherampaid,
    coalesce(p_amp.name, ap_amp.name) as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name           as advertiseramaaname,
    dwr.eventName
from (
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
    --    packageName,
    --    domain,
    --    operatingSystem,
    --    systemLanguage,
    --    deviceBrand,
    --    deviceType,
    --    browserKernel,
        respStatus,
        test,
        ruleId,
        smartId,
        eventName,
        date_format(b_time, 'yyyy-MM-dd 00:00:00') as b_time,
        date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time,
        b_date,
        sum(requestCount) as requestCount,
        sum(sendCount) as sendCount,
        sum(showCount) as showCount,
        sum(clickCount) as clickCount,
        sum(feeReportCount) as feeReportCount,  --计费条数
        sum(feeSendCount)   as feeSendCount,    --计费显示条数
        sum(feeReportPrice) as feeReportPrice,  --计费金额(真实收益)
        sum(feeSendPrice)   as feeSendPrice,    --计费显示金额(收益)
        sum(cpcBidPrice)    as cpcBidPrice,
        sum(cpmBidPrice)    as cpmBidPrice,
        sum(conversion)     as conversion,  --转化数，目前不要含展示和点击产生的
        sum(allConversion)  as allConversion, --转化数，含展示和点击产生的
        sum(revenue)        as revenue, --收益
        sum(realRevenue)    as realRevenue,     --真实收益
        sum(feeCpcTimes)    as feeCpcTimes,     -- fee cpc转化条数
        sum(feeCpmTimes)    as feeCpmTimes,     -- fee cpm转化条数
        sum(feeCpaTimes)    as feeCpaTimes,     -- fee cpa转化条数
        sum(feeCpaSendTimes)   as feeCpaSendTimes,       -- fee cpa send转化条数
        sum(feeCpcReportPrice) as feeCpcReportPrice,     -- fee cpc上游收益
        sum(feeCpmReportPrice) as feeCpmReportPrice,     -- fee cpm上游收益
        sum(feeCpaReportPrice) as feeCpaReportPrice,     -- fee cpa上游收益
        sum(feeCpcSendPrice)   as feeCpcSendPrice,       -- fee cpc下游收益
        sum(feeCpmSendPrice)   as feeCpmSendPrice,       -- fee cpm下游收益
        sum(feeCpaSendPrice)   as feeCpaSendPrice,     -- fee cpa下游收益
        sum(winPrice)          as winPrice,     -- 中签价格
        sum(winNotices)        as winNotices,    -- 中签数
        sum(newCount)          as newCount,
        sum(activeCount)       as activeCount
    from ssp_report_overall_dwr
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
    --    packageName,
    --    domain,
    --    operatingSystem,
    --    systemLanguage,
    --    deviceBrand,
    --    deviceType,
    --    browserKernel,
    respStatus,
    test,
    ruleId,
    smartId,
    eventName,
    date_format(b_time, 'yyyy-MM-dd 00:00:00'),
    date_format(l_time, 'yyyy-MM-dd 00:00:00'),
    b_date
) dwr
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
--    cast(null as string)        as packageName,
--    cast(null as string)        as domain,
--    cast(null as string)        as operatingSystem,
--    cast(null as string)        as systemLanguage,
--    cast(null as string)        as deviceBrand,
--    cast(null as string)        as deviceType,
--    cast(null as string)        as browserKernel,
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
-- Overall for day DM - END
--*******************************************************************************************************
