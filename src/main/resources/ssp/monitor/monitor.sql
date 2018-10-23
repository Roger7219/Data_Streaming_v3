
drop table if exists ssp_topn_dm ;
CREATE TABLE ssp_topn_dm (
        id                       INT,
        name                     STRING,
        publisherAmId            INT, -- 用于权限

        requestCount             BIGINT,
        clickCount               BIGINT,
        realConversion           BIGINT,
        conversion               BIGINT,
        realRevenue              DECIMAL(19,10),
        revenue                  DECIMAL(19,10), -- +
        realEcpc                 DOUBLE,
        ecpc                     DOUBLE,         -- +
        realCr                   DOUBLE,
        cr                       DOUBLE,

        yesterdayDate            STRING,
        yesterdayId              INT,
        yesterdayRequestCount    BIGINT,
        yesterdayClickCount      BIGINT,
        yesterdayRealConversion  BIGINT,
        yesterdayConversion      BIGINT,
        yesterdayRealRevenue     DECIMAL(19,10),
        yesterdayRevenue         DECIMAL(19,10), -- +
        yesterdayRealEcpc        DOUBLE,
        yesterdayEcpc            DOUBLE,         -- +
        yesterdayRealCr          DOUBLE,
        yesterdayCr              DOUBLE,

        realRevenueInc           DOUBLE,         -- +
        revenueInc               DOUBLE,
        realAbsRevenueInc        DOUBLE,
        absRevenueInc            DOUBLE,
        realCrInc                DOUBLE,
        crInc                    DOUBLE,
        realEcpcInc              DOUBLE,          --+
        ecpcInc                  DOUBLE
)
-- data_type: publisher:1, app:2, country:3, offer:4
PARTITIONED BY (data_type int, b_date STRING)
STORED AS ORC;



-- bd

drop table if exists bd_offer_dm ;
CREATE TABLE bd_offer_dm (
        countryId                   int,
        carrierId                   int,
        adcategory1Id               int,
        countryName                 string,
        carrierName                 string,
        adcategory1Name             string,
        offerCount                  BIGINT,
        revenue                     double,
        clickcount                  BIGINT,
        ecpc                        double,
        isOfferCountFew             int,
        isEcpcHigh                  int,
        isEcpcLow                   int
)
PARTITIONED BY (b_date STRING)
STORED AS ORC;


------------------------------------------------------------------------------------------
--
------------------------------------------------------------------------------------------

set b_date = "2017-11-13";
--set groupBy = publisher;

-- publisher
create or replace temporary  view publisher_t AS
select
    b_date,
    publisherAmId,
    publisherId          as id,
    max(publisherName)   as name,
    sum(requestCount)    as requestCount,
    sum(clickCount)      as clickCount,
    sum(feeReportCount)  as realConversion,
    sum(feeSendCount)    as conversion,
    sum(realRevenue)     as realRevenue,
    sum(revenue)         as revenue,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realEcpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(revenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0     as ecpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeReportCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realCr,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeSendCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0   as cr
from ssp_report_campaign_dm
where b_date in ( date_sub(${b_date}, 2), date_sub(${b_date}, 1) )
group by b_date, publisherAmId, publisherId;

create or replace temporary view publisher_y AS
select
    date_add(b_date, 1) as joinDate,
    b_date              as yesterdayDate,
    id                  as yesterdayId,
    name                as yesterdayName,
    requestCount        as yesterdayRequestCount,
    clickCount          as yesterdayClickCount,
    realConversion      as yesterdayRealConversion,
    conversion          as yesterdayConversion,
    realRevenue         as yesterdayRealRevenue,
    revenue             as yesterdayRevenue,
    realEcpc            as yesterdayRealEcpc,
    ecpc                as yesterdayEcpc,
    realCr              as yesterdayRealCr,
    cr                  as yesterdayCr
from publisher_t;

-- app
create or replace temporary  view app_t AS
select
    b_date,
    publisherAmId,
    appId                as id,
    max(appName)         as name,
    sum(requestCount)    as requestCount,
    sum(clickCount)      as clickCount,
    sum(feeReportCount)  as realConversion,
    sum(feeSendCount)    as conversion,
    sum(realRevenue)     as realRevenue,
    sum(revenue)         as revenue,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realEcpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(revenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0     as ecpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeReportCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realCr,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeSendCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0   as cr
from ssp_report_campaign_dm
where b_date in ( date_sub(${b_date}, 2), date_sub(${b_date}, 1)  )
group by b_date, publisherAmId, appId;

create or replace temporary view app_y AS
select
    date_add(b_date, 1) as joinDate,
    b_date              as yesterdayDate,
    id                  as yesterdayId,
    name                as yesterdayName,
    requestCount        as yesterdayRequestCount,
    clickCount          as yesterdayClickCount,
    realConversion      as yesterdayRealConversion,
    conversion          as yesterdayConversion,
    realRevenue         as yesterdayRealRevenue,
    revenue             as yesterdayRevenue,
    realEcpc            as yesterdayRealEcpc,
    ecpc                as yesterdayEcpc,
    realCr              as yesterdayRealCr,
    cr                  as yesterdayCr
from app_t;

-- country
create or replace temporary  view country_t AS
select
    b_date,
    publisherAmId,
    countryId            as id,
    max(countryName)     as name,
    sum(requestCount)    as requestCount,
    sum(clickCount)      as clickCount,
    sum(feeReportCount)  as realConversion,
    sum(feeSendCount)    as conversion,
    sum(realRevenue)     as realRevenue,
    sum(revenue)         as revenue,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realEcpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(revenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0     as ecpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeReportCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realCr,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeSendCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0   as cr
from ssp_report_campaign_dm
where b_date in ( date_sub(${b_date}, 2), date_sub(${b_date}, 1)  )
group by b_date, publisherAmId, countryId;

create or replace temporary view country_y AS
select
    date_add(b_date, 1) as joinDate,
    b_date              as yesterdayDate,
    id                  as yesterdayId,
    name                as yesterdayName,
    requestCount        as yesterdayRequestCount,
    clickCount          as yesterdayClickCount,
    realConversion      as yesterdayRealConversion,
    conversion          as yesterdayConversion,
    realRevenue         as yesterdayRealRevenue,
    revenue             as yesterdayRevenue,
    realEcpc            as yesterdayRealEcpc,
    ecpc                as yesterdayEcpc,
    realCr              as yesterdayRealCr,
    cr                  as yesterdayCr
from country_t;

-- offer
create or replace temporary  view offer_t AS
select
    b_date,
    publisherAmId,
    offerId              as id,
    max(offerName)       as name,
    sum(requestCount)    as requestCount,
    sum(clickCount)      as clickCount,
    sum(feeReportCount)  as realConversion,
    sum(feeSendCount)    as conversion,
    sum(realRevenue)     as realRevenue,
    sum(revenue)         as revenue,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realEcpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(revenue)/cast(sum(clickCount)  as double) END AS BIGINT)/100000.0     as ecpc,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeReportCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0 as realCr,
    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeSendCount) /cast(sum(clickCount)  as double) END AS BIGINT)/100000.0   as cr
from ssp_report_campaign_dm
where b_date in ( date_sub(${b_date}, 2), date_sub(${b_date}, 1) )
group by b_date, publisherAmId, offerId;

create or replace temporary view offer_y AS
select
    date_add(b_date, 1) as joinDate,
    b_date              as yesterdayDate,
    id                  as yesterdayId,
    name                as yesterdayName,
    requestCount        as yesterdayRequestCount,
    clickCount          as yesterdayClickCount,
    realConversion      as yesterdayRealConversion,
    conversion          as yesterdayConversion,
    realRevenue         as yesterdayRealRevenue,
    revenue             as yesterdayRevenue,
    realEcpc            as yesterdayRealEcpc,
    ecpc                as yesterdayEcpc,
    realCr              as yesterdayRealCr,
    cr                  as yesterdayCr
from offer_t;

-- DM
create or replace temporary view dm AS
select
    id,
    t.name as name,
    t.publisherAmId,
    t.requestCount,
    t.clickCount,
    t.realConversion,
    t.conversion,
    t.realRevenue,
    t.revenue,
    t.realEcpc,
    t.ecpc,
    t.realCr,
    t.cr,
    y.*,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue     - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )  as realRevenueInc,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue         - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 )          as revenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )) as realAbsRevenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue     - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 ))         as absRevenueInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realCr               - y.yesterdayRealCr)*100/t.realCr AS BIGINT)/100.0 )            as realCrInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.cr                   - y.yesterdayCr)*100/t.cr AS BIGINT)/100.0 )                    as crInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realEcpc           - y.yesterdayRealEcpc)*100/t.realEcpc AS BIGINT)/100.0 )        as realEcpcInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.ecpc               - y.yesterdayEcpc)*100/t.ecpc AS BIGINT)/100.0 )                as ecpcInc,
    1 as data_type,
    t.b_date
from publisher_t t
left join  publisher_y y on t.b_date = y.joinDate and t.id  = y.yesterdayId
union all
select
    id,
    t.name as name,
    t.publisherAmId,
    t.requestCount,
    t.clickCount,
    t.realConversion,
    t.conversion,
    t.realRevenue,
    t.revenue,
    t.realEcpc,
    t.ecpc,
    t.realCr,
    t.cr,
    y.*,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue     - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )  as realRevenueInc,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue         - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 )          as revenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )) as realAbsRevenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue     - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 ))         as absRevenueInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realCr               - y.yesterdayRealCr)*100/t.realCr AS BIGINT)/100.0 )            as realCrInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.cr                   - y.yesterdayCr)*100/t.cr AS BIGINT)/100.0 )                    as crInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realEcpc           - y.yesterdayRealEcpc)*100/t.realEcpc AS BIGINT)/100.0 )        as realEcpcInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.ecpc               - y.yesterdayEcpc)*100/t.ecpc AS BIGINT)/100.0 )                as ecpcInc,
    2 as data_type,
    t.b_date
from app_t t
left join  app_y y on t.b_date = y.joinDate and t.id  = y.yesterdayId
union all
select
    id,
    t.name as name,
    t.publisherAmId,
    t.requestCount,
    t.clickCount,
    t.realConversion,
    t.conversion,
    t.realRevenue,
    t.revenue,
    t.realEcpc,
    t.ecpc,
    t.realCr,
    t.cr,
    y.*,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue     - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )  as realRevenueInc,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue         - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 )          as revenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )) as realAbsRevenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue     - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 ))         as absRevenueInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realCr               - y.yesterdayRealCr)*100/t.realCr AS BIGINT)/100.0 )            as realCrInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.cr                   - y.yesterdayCr)*100/t.cr AS BIGINT)/100.0 )                    as crInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realEcpc           - y.yesterdayRealEcpc)*100/t.realEcpc AS BIGINT)/100.0 )        as realEcpcInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.ecpc               - y.yesterdayEcpc)*100/t.ecpc AS BIGINT)/100.0 )                as ecpcInc,
    3 as data_type,
    t.b_date
from country_t t
left join  country_y y on t.b_date = y.joinDate and t.id  = y.yesterdayId
union all
select
    id,
    t.name as name,
    t.publisherAmId,
    t.requestCount,
    t.clickCount,
    t.realConversion,
    t.conversion,
    t.realRevenue,
    t.revenue,
    t.realEcpc,
    t.ecpc,
    t.realCr,
    t.cr,
    y.*,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue     - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )  as realRevenueInc,
    if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue         - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 )          as revenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )) as realAbsRevenueInc,
    abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue     - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 ))         as absRevenueInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realCr               - y.yesterdayRealCr)*100/t.realCr AS BIGINT)/100.0 )            as realCrInc,
    if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.cr                   - y.yesterdayCr)*100/t.cr AS BIGINT)/100.0 )                    as crInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realEcpc           - y.yesterdayRealEcpc)*100/t.realEcpc AS BIGINT)/100.0 )        as realEcpcInc,
    if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.ecpc               - y.yesterdayEcpc)*100/t.ecpc AS BIGINT)/100.0 )                as ecpcInc,
    4 as data_type,
    t.b_date
from offer_t t
left join  offer_y y on t.b_date = y.joinDate and t.id  = y.yesterdayId
;
