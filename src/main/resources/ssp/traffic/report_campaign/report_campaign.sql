create table ssp_report_campaign_dwr(
       publisherId    INT,
       appId          INT,
       countryId      INT,
       carrierId      INT,
       versionName    STRING,         -- eg: v1.2.3
       adType         INT,
       campaignId     INT,
       offerId        INT,
       imageId        INT,
       affSub         STRING,
       requestCount   BIGINT,
       sendCount      BIGINT,
       showCount      BIGINT,
       clickCount     BIGINT,
       feeReportCount BIGINT,         -- 计费条数
       feeSendCount   BIGINT,         -- 计费显示条数
       feeReportPrice DECIMAL(19,10), -- 计费金额(真实收益)
       feeSendPrice   DECIMAL(19,10), -- 计费显示金额(收益)
       cpcBidPrice    DECIMAL(19,10),
       cpmBidPrice    DECIMAL(19,10),
       conversion     BIGINT,         -- 转化数，目前不要含展示和点击产生的
       allConversion  BIGINT,         -- 转化数，含展示和点击产生的
       revenue        DECIMAL(19,10), -- 收益
       realRevenue    DECIMAL(19,10) -- 真实收益
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add cloumn
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpcTimes BIGINT);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpmTimes BIGINT);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpaTimes BIGINT);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpaSendTimes BIGINT);

ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpcReportPrice decimal(19,10));
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpmReportPrice decimal(19,10));
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpaReportPrice decimal(19,10));

ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpcSendPrice decimal(19,10));
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpmSendPrice decimal(19,10));
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (feeCpaSendPrice decimal(19,10));

--add new clomns 12/21
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (domain STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_report_campaign_dwr ADD COLUMNS (b_time STRING);


-- MYSQL 10月前統計結果 for Campaign Reports
drop table if exists ADVER_STAT ;
CREATE TABLE ADVER_STAT (
  CampaignId        int,
  OfferId           int,
  CountryId         int,
  CarrierId         int,
  SdkVersion        STRING,
  AdType            int,
  SendCount         BIGINT,
  ShowCount         BIGINT,
  ClickCount        BIGINT,
  Fee               DECIMAL(15,5),
  FeeCount          int,
  ShowSendCount     int,
  ShowShowCount     int,
  ShowClickCount    int,
  SendFee           DECIMAL(15,5)  ,
  SendFeeCount      int
)
PARTITIONED BY ( StatDate STRING)
STORED AS ORC;

-- From MYSQL for App Reports
CREATE TABLE DATA_STAT (
  PublisherId       int,
  AppId             int,
  CampaignId        int,
  OfferId           int,
  CountryId         int,
  CarrierId         int,
  SdkVersion        STRING,
  AdType            int,
  SendCount         BIGINT,
  ShowCount         BIGINT,
  ClickCount        BIGINT,
  Fee               decimal(15,5),
  FeeCount          BIGINT,
  ShowSendCount     BIGINT,
  ShowShowCount     BIGINT,
  ShowClickCount    BIGINT,
  SendPercent       int,
  ShowPercent       int,
  ClickPercent      int,
  IsUpdate          int,
  SendFee           decimal(15,5),
  SendFeeCount      BIGINT
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;

--for Match Reports
CREATE TABLE DATA_FILL_STAT (
  PublisherId       int,
  AppId             int,
  CountryId         int,
  CarrierId         int,
  SdkVersion        STRING,
  AdType            int,
  TotalCount        BIGINT,
  SendCount         BIGINT
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;


-- Greenplum
create table ssp_report_campaign_dwr(
       "publisherId"    INT,
       "appId"          INT,
       "countryId"      INT,
       "carrierId"      INT,
       "versionName"    varchar(200),         -- eg: v1.2.3
       "adType"         INT,
       "campaignId"     INT,
       "offerId"        INT,
       "imageId"        INT,
       "affSub"         varchar(200),
       "requestCount"   BIGINT,
       "sendCount"      BIGINT,
       "showCount"      BIGINT,
       "clickCount"     BIGINT,
       "feeReportCount" BIGINT,         -- 计费条数
       "feeSendCount"   BIGINT,         -- 计费显示条数
       "feeReportPrice" DECIMAL(19,10), -- 计费金额(真实收益)
       "feeSendPrice"   DECIMAL(19,10), -- 计费显示金额(收益)
       "cpcBidPrice"    DECIMAL(19,10),
       "cpmBidPrice"    DECIMAL(19,10),
       "conversion"     BIGINT,         -- 转化数，目前不要含展示和点击产生的
       "allConversion"  BIGINT,         -- 转化数，含展示和点击产生的
       "revenue"        DECIMAL(19,10), -- 收益
       "realRevenue"    DECIMAL(19,10), -- 真实收益
       "l_time"         varchar(200),
       "b_date"         varchar(200)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES(null)  WITH (tablename='ssp_report_campaign_dwr_default_partition')
);

-- Overall/Match/Campaign公用视图

-- FOR Kylin
drop view if exists ssp_report_campaign_dm;
create view ssp_report_campaign_dm as
select
    dwr.publisherId   ,--INT,
    dwr.appId         ,--INT,
    dwr.countryId     ,--INT,
    dwr.carrierId     ,--INT,
--    dwr.versionName   ,-- STRING,         -- eg: v1.2.3
    dwr.adType        ,--INT,
    dwr.campaignId    ,--INT,
    dwr.offerId       ,--INT,
    dwr.imageId       ,--INT,
    dwr.affSub        ,-- STRING,
    dwr.requestCount  ,-- BIGINT,
    dwr.sendCount     ,-- BIGINT,
    dwr.showCount     ,-- BIGINT,
    dwr.clickCount    ,--BIGINT,
    dwr.feeReportCount,-- BIGINT,         -- 计费条数
    dwr.feeSendCount  ,--BIGINT,         -- 计费显示条数
    dwr.feeReportPrice,-- DECIMAL(19,10), -- 计费金额(真实收益)
    dwr.feeSendPrice  ,-- DECIMAL(19,10), -- 计费显示金额(收益)
    dwr.cpcBidPrice   ,-- DECIMAL(19,10),
    dwr.cpmBidPrice   ,-- DECIMAL(19,10),
    dwr.conversion    ,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    dwr.allConversion ,-- BIGINT,         -- 转化数，含展示和点击产生的
    dwr.revenue       ,-- DECIMAL(19,10), -- 收益
    dwr.realRevenue   ,-- DECIMAL(19,10) -- 真实收益
    dwr.l_time,
    dwr.b_date,
    p.amId                                                     as publisherAmId,
    concat_ws('^', p_am.name , cast(p.amId as string) )        as publisherAmName,
    ad.amId                                                    as advertiserAmId,
    concat_ws('^', a_am.name , cast(ad.amId as string) )       as advertiserAmName,
    a.mode                                                     as appModeId,
    concat_ws('^', m.name , cast(a.mode as string) )           as appModeName,
    cam.adCategory1                                            as adCategory1Id, --INT 关联campaign.adCategory1
    concat_ws('^', adc.name , cast(cam.adCategory1 as string) )as adCategory1Name,
    concat_ws('^', cam.name , cast(dwr.campaignId as string) ) as campaignName,
    cam.adverId,
    concat_ws('^', ad.name , cast(cam.adverid as string) )     as adverName,
    o.optStatus                                                as offerOptStatus,
    concat_ws('^', o.name , cast(dwr.offerId as string) )      as offerName,
    concat_ws('^', p.name , cast(dwr.publisherId as string) )  as publisherName,
    concat_ws('^', a.name , cast(dwr.appId as string) )        as appName,
    concat_ws('^', c.name , cast(dwr.countryId as string) )    as countryName,
    concat_ws('^', ca.name , cast(dwr.carrierId as string) )   as carrierName,
    dwr.adType                                                 as adTypeId,   --adFormatId
    concat_ws('^', adt.name , cast(dwr.adType as string) )     as adTypeName,
    v.id                                                       as versionId,
    concat_ws('^', dwr.versionName, cast(v.id as string) )     as versionName,
    p_am.proxyId                                               as publisherProxyId
from ssp_report_campaign_dwr dwr
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
where v.id is not null;

-- Greenplum
drop view ssp_report_campaign_dm;
create view ssp_report_campaign_dm as
select
    dwr."publisherId"   ,--INT,
    dwr."appId"         ,--INT,
    dwr."countryId"     ,--INT,
    dwr."carrierId"     ,--INT,
--    dwr.versionName   ,-- STRING,         -- eg: v1.2.3
    dwr."adType"        ,--INT,
    dwr."campaignId"    ,--INT,
    dwr."offerId"       ,--INT,
    dwr."imageId"       ,--INT,
    dwr."affSub"        ,-- STRING,
    dwr."requestCount"  ,-- BIGINT,
    dwr."sendCount"     ,-- BIGINT,
    dwr."showCount"     ,-- BIGINT,
    dwr."clickCount"    ,--BIGINT,
    dwr."feeReportCount",-- BIGINT,         -- 计费条数
    dwr."feeSendCount"  ,--BIGINT,         -- 计费显示条数
    dwr."feeReportPrice",-- DECIMAL(19,10), -- 计费金额(真实收益)
    dwr."feeSendPrice"  ,-- DECIMAL(19,10), -- 计费显示金额(收益)
    dwr."cpcBidPrice"   ,-- DECIMAL(19,10),
    dwr."cpmBidPrice"   ,-- DECIMAL(19,10),
    dwr."conversion"    ,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    dwr."allConversion" ,-- BIGINT,         -- 转化数，含展示和点击产生的
    dwr."revenue"       ,-- DECIMAL(19,10), -- 收益
    dwr."realRevenue"   ,-- DECIMAL(19,10) -- 真实收益
    dwr."l_time",
    dwr."b_date",
    p.amId            as "publisherAmId",
    p_am.name         as "publisherAmName",
    ad.amId           as "advertiserAmId",
    a_am.name         as "advertiserAmName",
    a.mode            as "appModeId",
    m.name            as "appModeName",
    cam.adCategory1   as "adCategory1Id", --INT 关联campaign.adCategory1
    adc.name          as "adCategory1Name",
    cam.name          as "campaignName",
    cam.adverId       as "adverId",
    ad.name           as "adverName",
    o.optStatus       as "offerOptStatus",
    o.name            as "offerName",
    p.name            as "publisherName",
    a.name            as "appName",
    c.name            as "countryName",
    ca.name           as "carrierName",
    dwr."adType"      as "adTypeId",   --adFormatId
    adt.name          as "adTypeName",
    v.id              as "versionId",
    dwr."versionName"   as "versionName",
    p_am.proxyId      as "publisherProxyId"
from ssp_report_campaign_dwr dwr
left join campaign cam      on cam.id = dwr."campaignId"
left join advertiser ad     on ad.id = cam.adverId
left join employee a_am     on a_am.id = ad.amid
left join offer o           on o.id = dwr."offerId"
left join publisher p       on p.id = dwr."publisherId"
left join employee p_am     on p_am.id = p.amid
left join app a             on a.id = dwr."appId"
left join app_mode m        on m.id = a.mode
left join country c         on c.id = dwr."countryId"
left join carrier ca        on ca.id = dwr."carrierId"
left join ad_type adt       on adt.id = dwr."adType"
left join version_control v on v.version = dwr."versionName"
left join ad_category1 adc  on adc.id =  cam.adCategory1
where v.id is  not null;

--select count(1), "offerId" from ssp_report_campaign_dwr  group by  "offerId" limit 11 offset 10 ;
--select count(1), "offerId" from ssp_report_campaign_dm  group by  "offerId" limit 11 offset 10 ;
--select count(1), appid from ssp_report_publisher_dm  group by  appid limit 11 offset 10 ;


-- Hive For bigquerry
drop view ssp_report_campaign_dm;
create view ssp_report_campaign_dm as
select
    dwr.publisherId   as publisherid,--INT,
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
    dwr.l_time,
    dwr.b_date,

    p.amId            as publisheramid,
    p_am.name         as publisheramname,
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
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,
    dwr.adType        as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
    dwr.versionName   as versionname,
    p_am.proxyId      as publisherproxyid,
    cast(null as string)    as data_type,
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
    dwr.b_time,
    co.id    as companyid,
    co.name  as companyname,
    p.ampaId as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name          as advertiseramaaname
from ssp_report_campaign_dwr dwr
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
--left join employee ap_amp   on ap_amp.id  = a_p.ampaid
left join employee a_ama    on a_ama.id = ad.amaaid
where v.id is not null and b_date >= "2017-12-18"

union all
select
    -1                      as publisherid,--INT,
    -1                      as appid,--INT,
    t.countryId             as countryid,--INT,
    t.carrierId             as carrierid,--INT,
    t.adType                as adtype,--INT,
    t.campaignId            as campaignid,--INT,
    t.offerId               as offerid,--INT,
    -1                      as imageid,--INT,
    null                    as affsub,-- STRING,
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
    0                       as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
    t.sendfee               as revenue,-- DECIMAL(19,10), -- 收益
    t.fee                   as realrevenue,-- DECIMAL(19,10) -- 真实收益
    '2000-01-01 00:00:00'   as l_time,
    t.statDate              as b_date,

    -1                as publisheramid,
    null              as publisheramname,
    ad.amId           as advertiseramid,
    a_am.name         as advertiseramname,
    -1                as appmodeid,
    null              as appmodename,
    cam.adCategory1   as adcategory1id, --INT 关联campaign.adCategory1
    adc.name          as adcategory1name,
    cam.name          as campaignname,
    cam.adverId       as adverid,
    ad.name           as advername,
    o.optStatus       as offeroptstatus,
    o.name            as offername,
    null              as publishername,
    null              as appname,
    c.name            as countryname,
    ca.name           as carriername,
    t.adType          as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
    t.sdkversion      as versionname,
    a_am.proxyId      as publisherproxyid,
    'camapgin'        as data_type,
    0                 as feecpctimes,--cpc计费转化数
    0                 as feecpmtimes,
    0                 as feecpatimes,
    0                 as feecpasendtimes,--cpc 计费下发数
    0                 as feecpcreportprice,--cpc 计费上游收益
    0                 as feecpmreportprice,
    0                 as feecpareportprice,
    0                 as feecpcsendprice,
    0                 as feecpmsendprice,
    0                 as feecpasendprice,
    c.alpha2_code     as countrycode,

    null        as packageName,
    null        as domain,
    null        as operatingSystem,
    null        as systemLanguage,
    null        as deviceBrand,
    null        as deviceType,
    null        as browserKernel,
    concat(statDate, ' 00:00:00') as b_time,
    0           as companyid,
    null        as companyname,
    cast(null as int) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    cast(null as string) as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name          as advertiseramaaname
from ADVER_STAT t
left join campaign cam      on cam.id = t.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join employee a_am     on a_am.id = ad.amid
left join offer o           on o.id = t.offerId
--left join publisher p       on p.id = dwr.publisherId
--left join employee p_am     on p_am.id = p.amid
--left join app a             on a.id = dwr.appId
--left join app_mode m        on m.id = a.mode
left join country c         on c.id = t.countryId
left join carrier ca        on ca.id = t.carrierId
left join ad_type adt       on adt.id = t.adType
left join version_control v on v.version = t.sdkversion
left join ad_category1 adc  on adc.id =  cam.adCategory1

left join employee a_ama    on a_ama.id = ad.amaaId

where v.id is not null and statdate < "2017-12-18"
union all
select
    x.publisherId   as publisherid,--INT,
    x.appId         as appid,--INT,
    x.countryId     as countryid,--INT,
    x.carrierId     as carrierid,--INT,
--    x.versionName   ,-- STRING,         -- eg: v1.2.3
    x.adType        as adtype,--INT,

    -1              as campaignid,--INT,
    -1              as offerid,--INT,
    -1              as imageid,--INT,
    null            as affsub,-- STRING,
    x.totalcount    as requestcount,-- BIGINT,
    x.sendCount     as sendcount,-- BIGINT,
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
    '2000-01-01 00:00:00'   as l_time,
    x.statdate as b_date,

    p.amId            as publisheramid,
    p_am.name         as publisheramname,
    -1                as advertiseramid,
    null              as advertiseramname,
    a.mode            as appmodeid,
    m.name            as appmodename,
    -1                as adcategory1id, --INT 关联campaign.adCategory1
    null              as adcategory1name,
    null              as campaignname,
    -1                as adverid,
    null              as advername,
    -1                as offeroptstatus,
    null              as offername,
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,
    x.adType          as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
    x.sdkversion      as versionname,
    p_am.proxyId      as publisherproxyid,
    'match'           as data_type,
    0                 as feecpctimes,--cpc计费转化数
    0                 as feecpmtimes,
    0                 as feecpatimes,
    0                 as feecpasendtimes,--cpc 计费下发数
    0                 as feecpcreportprice,--cpc 计费上游收益
    0                 as feecpmreportprice,
    0                 as feecpareportprice,
    0                 as feecpcsendprice,
    0                 as feecpmsendprice,
    0                 as feecpasendprice,
    c.alpha2_code     as countrycode,

    null        as packageName,
    null        as domain,
    null        as operatingSystem,
    null        as systemLanguage,
    null        as deviceBrand,
    null        as deviceType,
    null        as browserKernel,
    concat(statDate, ' 00:00:00') as b_time,
    co.id       as companyid,
    co.name     as companyname,
    p.ampaId as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name as publisherampaname,
    cast(null as int)    as advertiseramaaid,
    cast(null as string) as advertiseramaaname
from data_fill_stat x
--left join campaign cam      on cam.id = x.campaignId
--left join advertiser ad     on ad.id = cam.adverId
--left join offer o           on o.id = x.offerId
left join publisher p       on p.id = x.publisherId
left join employee p_am     on p_am.id = p.amid
left join app a             on a.id = x.appId
left join app_mode m        on m.id = a.mode
left join country c         on c.id = x.countryId
left join carrier ca        on ca.id = x.carrierId
left join ad_type adt       on adt.id = x.adType
left join version_control v on v.version = x.sdkversion
--left join ad_category1 adc  on adc.id =  cam.adCategory1

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId

where v.id is not null and statdate < "2017-12-18"
union all
select
    ds.publisherId   as publisherid,--INT,
    ds.appId         as appid,--INT,
    ds.countryId     as countryid,--INT,
    ds.carrierId     as carrierid,--INT,
    ds.adType        as adtype,--INT,
    ds.campaignId    as campaignid,--INT,
    ds.offerId       as offerid,--INT,
    -1               as imageid,--INT,
    null             as affsub,-- STRING,
    0                as requestcount,-- BIGINT,
    ds.sendCount     as sendcount,-- BIGINT,
    ds.showCount     as showcount,-- BIGINT,
    ds.clickCount    as clickcount,--BIGINT,
    ds.feecount      as feereportcount,-- BIGINT,         -- 计费条数
    ds.sendfeecount   as feesendcount,--BIGINT,         -- 计费显示条数
    ds.fee            as feereportprice,-- DECIMAL(19,10), -- 计费金额(真实收益)
    ds.sendfee        as feesendprice,-- DECIMAL(19,10), -- 计费显示金额(收益)
    0                as cpcbidprice,-- DECIMAL(19,10),
    0                as cpmbidprice,-- DECIMAL(19,10),
    ds.feecount      as conversion,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    0                as allconversion,-- BIGINT,         -- 转化数，含展示和点击产生的
    ds.sendfee       as revenue,-- DECIMAL(19,10), -- 收益
    ds.fee           as realrevenue,-- DECIMAL(19,10) -- 真实收益
    '2000-01-01 00:00:00'   as l_time,
    ds.statdate as b_date,

    p.amId            as publisheramid,
    p_am.name         as publisheramname,
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
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,
    ds.adType         as adtypeid,   --adFormatId
    adt.name          as adtypename,
    v.id              as versionid,
    ds.sdkversion     as versionname,
    p_am.proxyId      as publisherproxyid,
    'overall'         as data_type,
    0                 as feecpctimes,--cpc计费转化数
    0                 as feecpmtimes,
    0                 as feecpatimes,
    0                 as feecpasendtimes,--cpc 计费下发数
    0                 as feecpcreportprice,--cpc 计费上游收益
    0                 as feecpmreportprice,
    0                 as feecpareportprice,
    0                 as feecpcsendprice,
    0                 as feecpmsendprice,
    0                 as feecpasendprice,
    c.alpha2_code     as countrycode,

    null        as packageName,
    null        as domain,
    null        as operatingSystem,
    null        as systemLanguage,
    null        as deviceBrand,
    null        as deviceType,
    null        as browserKernel,
    concat(statDate, ' 00:00:00') as b_time,
    co.id       as companyid,
    co.name     as companyname,
    p.ampaId    as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name  as publisherampaname,
    ad.amaaId           as advertiseramaaid,
    a_ama.name          as advertiseramaaname
from data_stat ds
left join campaign cam      on cam.id = ds.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join employee a_am     on a_am.id = ad.amid
left join offer o           on o.id = ds.offerId
left join publisher p       on p.id = ds.publisherId
left join employee p_am     on p_am.id = p.amid
left join app a             on a.id = ds.appId
left join app_mode m        on m.id = a.mode
left join country c         on c.id = ds.countryId
left join carrier ca        on ca.id = ds.carrierId
left join ad_type adt       on adt.id = ds.adType
left join version_control v on v.version = ds.sdkversion
left join ad_category1 adc  on adc.id =  cam.adCategory1

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId
where v.id is not null and statdate < "2017-12-18" ;


-- and (dwr.sendCount>0 or dwr.showCount>0 or dwr.clickCount>0 or dwr.realRevenue>0);


--Greenplum
create table ssp_report_campaign_dm (
    publisherId       integer,
    appId             integer,
    countryId         integer,
    carrierId         integer,
    adType            integer             ,
    campaignId        integer             ,
    offerId           integer             ,
    imageId           integer             ,
    affSub            character varying(200),
    requestCount      bigint              ,
    sendCount         bigint              ,
    showCount         bigint              ,
    clickCount        bigint              ,
    feeReportCount    bigint              ,
    feeSendCount      bigint              ,
    feeReportPrice    numeric(19,10)      ,
    feeSendPrice      numeric(19,10)      ,
    cpcBidPrice       numeric(19,10)      ,
    cpmBidPrice       numeric(19,10)      ,
    conversion        bigint              ,
    allConversion     bigint              ,
    revenue           numeric(19,10)      ,
    realRevenue       numeric(19,10)      ,
    l_time            character varying(200),
    b_date            character varying(200),

    publisherAmId     integer               ,
    publisherAmName   character varying(100),
    advertiserAmId    integer               ,
    advertiserAmName  character varying(100),
    appModeId         integer               ,
    appModeName       character varying(100),
    adCategory1Id     integer               ,
    adCategory1Name   character varying(100),
    campaignName      character varying(100),
    adverId           integer               ,
    adverName         character varying(100),
    offerOptStatus    integer               ,
    offerName         character varying(100),
    publisherName     character varying(100),
    appName           character varying(100),
    countryName       character varying(100),
    carrierName       character varying(100),
    adTypeId          integer               ,
    adTypeName        character varying(100),
    versionId         integer               ,
    versionName       character varying(200),
    publisherProxyId  integer
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES(null)  WITH (tablename='ssp_report_campaign_dm_default_partition')
);

-- Greenplum
create table ad_type(
   id   INT,
   name varchar(100)
);

insert overwrite table ad_type
values
( 2, 'Mobile-320×50'),
( 3, 'Mobile-300×250'),
( 4, 'Interstitial'),
( 5, 'Mobile-320×480'),
( 6, 'Mobile-1200*627'),
( 7, 'AdType-Id-7'),
( 8, 'AdType-Id-8'),
( 9, 'Mobile-600x600'),
( 10, 'Mobile-720x290');


-- Greenplum
CREATE TABLE app_mode(id INT, name varchar(100));
INSERT INTO app_mode
VALUES
(1, 'SDK'),
(2, 'Smartlink'),
(3, 'Online API'),
(4, 'JS Tag'),
(5, 'RTB'),
(6, 'CPA Offline API'),
(7, 'Video Sdk')
(8, 'CPI Offline API')
;

-- Greenplum
CREATE TABLE ad_category1(id int, name varchar(100));
INSERT INTO ad_category1
VALUES
(1, 'Adult'),
(2, 'Mainstream');


select sum(revenue), sum(realRevenue),sum(requestCount),b_date
from ssp_report_campaign_dwr
group by b_date
order by b_date desc;



select sum(revenue), sum(realRevenue),sum(requestCount),b_date
from ssp_report_app_dwr
group by b_date
order by b_date desc;


-- report_app 也用app_mode
create table app_mode(id INT, name STRING) STORED AS ORC;
INSERT OVERWRITE TABLE app_mode
VALUES
(1, 'SDK'),
(2, 'Smartlink'),
(3, 'Online API'),
(4, 'JS Tag'),
(5, 'RTB'),
(6, 'CPA Offline API'),
(7, 'Video Sdk'),
(8, 'CPI Offline API'),
(9, 'Market')
;

create table ad_category1(id int, name STRING) STORED AS ORC;
INSERT OVERWRITE TABLE ad_category1
VALUES
(1, 'Adult'),
(2, 'Mainstream');


------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_report_campaign_dwr_view;
create view ssp_report_campaign_dwr_view as
select
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
    adType,
    campaignId,
    offerId,
    imageId,
    affSub,
    sum(requestCount)       as requestCount,
    sum(sendCount)          as sendCount,
    sum(showCount)          as showCount,
    sum(clickCount)         as clickCount,
    sum(feeReportCount)     as feeReportCount,
    sum(feeSendCount)       as feeSendCount,
    sum(feeReportPrice)     as feeReportPrice,
    sum(feeSendPrice)       as feeSendPrice,
    sum(cpcBidPrice)        as cpcBidPrice,
    sum(cpmBidPrice)        as cpmBidPrice,
    sum(conversion)         as conversion,
    sum(allConversion)      as allConversion,
    sum(revenue)            as revenue,
    sum(realRevenue)        as realRevenue,
    sum(feeCpcTimes)        as feecpctimes,--cpc计费转化数
    sum(feeCpmTimes)        as feecpmtimes,
    sum(feeCpaTimes)        as feecpatimes,
    sum(feeCpaSendTimes)    as feecpasendtimes,--cpc 计费下发数
    sum(feeCpcReportPrice)  as feecpcreportprice,--cpc 计费上游收益
    sum(feeCpmReportPrice)  as feecpmreportprice,
    sum(feeCpaReportPrice)  as feecpareportprice,
    sum(feeCpcSendPrice)    as feecpcsendprice,
    sum(feeCpmSendPrice)    as feecpmsendprice,
    sum(feeCpaSendPrice)    as feecpasendprice,

    packageName,
    domain,
    operatingSystem,
    systemLanguage,
    deviceBrand,
    deviceType,
    browserKernel,
    b_time,

    l_time,
    b_date
from (

    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        times      as requestCount,
        0          as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        0          as feecpctimes,--cpc计费转化数
        0          as feecpmtimes,
        0          as feecpatimes,
        0          as feecpasendtimes,--cpc 计费下发数
        0          as feecpcreportprice,--cpc 计费上游收益
        0          as feecpmreportprice,
        0          as feecpareportprice,
        0          as feecpcsendprice,
        0          as feecpmsendprice,
        0          as feecpasendprice,

        packageName,
        domain,
        operatingSystem,
        systemLanguage,
        deviceBrand,
        deviceType,
        browserKernel,
        b_time,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fill_dwr
    where second(l_time) = 0
--    group by
--        publisherId,
--        subId,
--        countryId,
--        carrierId,
--        sv,
--        adType,
--        campaignId,
--        offerId,
--        imageId,
--        affSub,
--        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        0          as requestCount,
        times      as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        0          as feecpctimes,--cpc计费转化数
        0          as feecpmtimes,
        0          as feecpatimes,
        0          as feecpasendtimes,--cpc 计费下发数
        0          as feecpcreportprice,--cpc 计费上游收益
        0          as feecpmreportprice,
        0          as feecpareportprice,
        0          as feecpcsendprice,
        0          as feecpmsendprice,
        0          as feecpasendprice,

        packageName,
        domain,
        operatingSystem,
        systemLanguage,
        deviceBrand,
        deviceType,
        browserKernel,
        b_time,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_send_dwr
    where second(l_time) = 0
--    group by
--        publisherId,
--        subId,
--        countryId,
--        carrierId,
--        sv,
--        adType,
--        campaignId,
--        offerId,
--        imageId,
--        affSub,
--        b_date
--
    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        0                 as requestCount,
        0                 as sendCount,
        times             as showCount,
        0                 as clickCount,
        0                 as feeReportCount,
        0                 as feeSendCount,
        0                 as feeReportPrice,
        0                 as feeSendPrice,
        0                 as cpcBidPrice,
        cpmBidPrice       as cpmBidPrice,
        0                 as conversion,
        cpmTimes     as allConversion,
        cpmSendPrice as revenue,
        cpmBidPrice  as realRevenue,
        0          as feecpctimes,--cpc计费转化数
        0          as feecpmtimes,
        0          as feecpatimes,
        0          as feecpasendtimes,--cpc 计费下发数
        0          as feecpcreportprice,--cpc 计费上游收益
        0          as feecpmreportprice,
        0          as feecpareportprice,
        0          as feecpcsendprice,
        0          as feecpmsendprice,
        0          as feecpasendprice,

        packageName,
        domain,
        operatingSystem,
        systemLanguage,
        deviceBrand,
        deviceType,
        browserKernel,
        b_time,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_show_dwr
    where second(l_time) = 0
--    group by
--        publisherId,
--        subId,
--        countryId,
--        carrierId,
--        sv,
--        adType,
--        campaignId,
--        offerId,
--        imageId,
--        affSub,
--        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        0                 as requestCount,
        0                 as sendCount,
        0                 as showCount,
        times             as clickCount,
        0                 as feeReportCount,
        0                 as feeSendCount,
        0                 as feeReportPrice,
        0                 as feeSendPrice,
        cpcBidPrice  as cpcBidPrice,
        0                 as cpmBidPrice,
        0                 as conversion,
        cpcTimes     as allConversion,
        cpcSendPrice as revenue,
        cpcBidPrice  as realRevenue,
        0          as feecpctimes,--cpc计费转化数
        0          as feecpmtimes,
        0          as feecpatimes,
        0          as feecpasendtimes,--cpc 计费下发数
        0          as feecpcreportprice,--cpc 计费上游收益
        0          as feecpmreportprice,
        0          as feecpareportprice,
        0          as feecpcsendprice,
        0          as feecpmsendprice,
        0          as feecpasendprice,

        packageName,
        domain,
        operatingSystem,
        systemLanguage,
        deviceBrand,
        deviceType,
        browserKernel,
        b_time,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_click_dwr
    where second(l_time) = 0
--    group by
--        publisherId,
--        subId,
--        countryId,
--        carrierId,
--        sv,
--        adType,
--        campaignId,
--        offerId,
--        imageId,
--        affSub,
--        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        adType,
        campaignId,
        offerId,
        imageId,
        affSub,
        0                 as requestCount,
        0                 as sendCount,
        0                 as showCount,
        0                 as clickCount,
        times             as feeReportCount,
        sendTimes         as feeSendCount,
        reportPrice       as feeReportPrice,
        sendPrice         as feeSendPrice,
        0                 as cpcBidPrice,
        0                 as cpmBidPrice,
        times        as conversion,
        times        as allConversion,
        sendPrice    as revenue,
        reportPrice  as realRevenue,
        feecpctimes,--cpc计费转化数
        feecpmtimes,
        feecpatimes,
        feecpasendtimes,--cpc 计费下发数
        feecpcreportprice,--cpc 计费上游收益
        feecpmreportprice,
        feecpareportprice,
        feecpcsendprice,
        feecpmsendprice,
        feecpasendprice,

        packageName,
        domain,
        operatingSystem,
        systemLanguage,
        deviceBrand,
        deviceType,
        browserKernel,
        b_time,

        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fee_dwr
    where second(l_time) = 0
--    group by
--        publisherId,
--        subId,
--        countryId,
--        carrierId,
--        sv,
--        adType,
--        campaignId,
--        offerId,
--        imageId,
--        affSub,
--        b_date
)t0
group by
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
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
    b_time,

    l_time,
    b_date;


set  spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

--CREATE TABLE IF NOT EXISTS ssp_report_campaign_dwr_tmp like ssp_report_campaign_dwr;
insert overwrite table ssp_report_campaign_dwr partition(l_time, b_date)
select * from ssp_report_campaign_dwr_view;
--where l_time <'2017-09-10';

