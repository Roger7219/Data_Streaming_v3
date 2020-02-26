drop view if exists smartlink_dm;

create view smartlink_dm as
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
    dwr.raterType     as ratertype, -- +++++
    dwr.raterId       as raterid,   -- +++++
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
from smartlink_dwr dwr
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
left join employee a_ama    on a_ama.id = ad.amaaId;



    SELECT O.ID id, O.`Name` name, C.`Name` campaignName, A.`Name` adName, O.CountryIds countryIds, O.BidPrice         bidPriceS,         O.price, O.AdType adType,O.PublisherPayout,O.Preview,O.Caps caps,O.TodayCaps todayCaps,O.ShowTimes,O.TodayClickCount,         CASE O.Status WHEN '0' THEN 'Disabled' WHEN '1' THEN 'Enabled' END statusName,         CASE O.AmStatus WHEN '0' THEN 'Pending' WHEN '1' THEN 'Approved' WHEN '2' THEN 'Deny' END amStatusName,         CASE C.adCategory1         WHEN 1 THEN "Adult"         WHEN 2 THEN "Mainstream"         END adCategory,         CASE O.IsApi WHEN '0' THEN 'NO' WHEN '1' THEN 'YES' END isApiName,         O.Modes modes,O.Level level,         DATE_FORMAT(O.CreateTime,'%Y-%m-%d') createTime1,         CASE C.adCategory2         WHEN '101' THEN 'Adultdating'         WHEN '102' THEN 'Gay'         WHEN '103' THEN 'Straight'         WHEN '201' THEN 'Consumergoods'         WHEN '202' THEN 'Dating'         WHEN '203' THEN 'Education'         WHEN '204' THEN 'Entertainment'         WHEN '205' THEN 'Finance'         WHEN '206' THEN 'Gambling/casino'         WHEN '207' THEN 'Games'         WHEN '208' THEN 'Leadgeneration'         WHEN '209' THEN 'Mobilecontentsubscription'         WHEN '210' THEN 'Mobilesoftware'         WHEN '211' THEN 'Shopping retail'         WHEN '212' THEN 'Social'         WHEN '213' THEN 'Sweepstakes'         WHEN '214' THEN 'Travel'         WHEN '215' THEN 'Utilities tools'         WHEN '216' THEN 'GP'         WHEN '217' THEN 'Smart contents'         WHEN '218' THEN 'Nutra'         WHEN '219' THEN 'Video'         WHEN '220' THEN 'Music'         WHEN '221' THEN 'Antivirus'         WHEN '222' THEN 'DDL'         END adCategory2Name,         CASE O.OptStatus         WHEN 0 THEN "Not analyzed"         WHEN 1 THEN "Optimizing"         WHEN 2 THEN "Fail"         WHEN 3 THEN "Success"         WHEN 4 THEN "Applying"         END optStatusString,         CASE O.spyStatus         WHEN '1' THEN 'No'         WHEN '2' THEN 'Optimizing'         WHEN 3 THEN "Success"         WHEN 4 THEN "Fail"         END spyStatusString,         IFNULL(DATE_FORMAT(O.OptTime,'%Y-%m-%d'),'--') optTime,         O.CarrierIds carrierIds, O.ImageUrl imageUrl, O.Url url,         O.`Desc`, O.adDesc adDesc, O.OptStatus optStatus, O.CampaignId,         O.OpType, O.SubIds, O.Status ,O.AmStatus, O.IsApi isApi,         CASE O.ConversionProcess         WHEN 1 THEN '0 Click'         WHEN 2 THEN '1 Click'         WHEN 3 THEN "2 Click"         WHEN 4 THEN "SOI"         WHEN 5 THEN "DOI"         WHEN 6 THEN "OTP"         WHEN 7 THEN "MO"         WHEN 8 THEN "PIN"         WHEN 9 THEN "Download"         WHEN 10 THEN 'CC Submit'         END conversionProcessName         FROM OFFER O use index(ix_offer_index)         LEFT JOIN CAMPAIGN C ON C.ID = O.CampaignId         LEFT JOIN ADVERTISER A ON A.ID = C.AdverId         LEFT JOIN EMPLOYEE E ON E.ID = A.AmId         LEFT JOIN EMPLOYEE EA ON EA.ID = A.AmaaId         WHERE                                                        O.Status = 1                         AND                 O.AmStatus = 1                                                                                                                                                                                                                                                            ORDER BY                                                                                                                                                                       LIMIT  0 10
