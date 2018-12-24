alter table add column

----------------------sql for topn-------------------------------
-- set b_date = "2017-11-13";
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
create or replace temporary view ssp_topn_dm AS
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

----------------------sql for topn end-------------------------------


----------------------sql for bd_offer-------------------------------

--set b_date=("2017-11-14");
--
create or replace temporary view bd_offer_dm_tmp as
select
  dm.b_date,
  dm.countryid,
  dm.carrierid,
  dm.adcategory1Id,
  count(distinct(dm.offerid)) as offercount,
  sum(dm.realrevenue) as revenue,
  sum(dm.clickcount) as clickcount,
  CAST( CASE sum(dm.clickcount)  WHEN 0 THEN 0 ELSE 100000*1000*sum(dm.revenue)/cast(sum(dm.clickcount)  as double) END AS BIGINT)/100000.0 as ecpc,
  max(dm.countryname) as countryname,
  max(dm.carriername) as carriername,
  max(dm.adcategory1name) as adcategory1Name
from ssp_report_campaign_dm dm
where b_date in ${b_date}
group by
  dm.b_date,dm.countryid,dm.carrierid,dm.adcategory1id
;

create or replace temporary view bd_offer_dm as
select
  dm.countryid,
  dm.carrierid,
  dm.adcategory1Id ,
  dm.countryname,
  dm.carriername,
  dm.adcategory1Name,
  dm.offercount,
  dm.revenue,
  dm.clickcount,
  dm.ecpc,
  if(dm.offercount < o.offercount, 1, 0)     as isoffercountfew,
  if( dm.ecpc > o.ecpchigh, 1, 0)            as isecpchigh,
  if(dm.ecpc < o.ecpclow, 1, 0)              as isecpclow,
  dm.b_date
from bd_offer_dm_tmp dm
left join offer_demand_config o
   on dm.countryid = o.countryid and dm.carrierid = o.carrierid and dm.adcategory1id = o.category
where (dm.clickcount > o.clickcount) and (o.carrierid is not null and o.category is not null and o.countryid is not null )
;

----------------------sql for bd_offer end---------------------------

