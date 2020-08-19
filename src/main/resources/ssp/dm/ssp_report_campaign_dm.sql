drop view if exists ssp_report_campaign_dm;
create view ssp_report_campaign_dm as
select
  publisherid         ,
  appid               ,
  countryid           ,
  carrierid           ,
  adtype              ,
  campaignid          ,
  offerid             ,
  imageid             ,
  affsub              ,
  requestcount        ,
  sendcount           ,
  showcount           ,
  clickcount          ,
  feereportcount      ,
  feesendcount        ,
  feereportprice      ,
  feesendprice        ,
  cpcbidprice         ,
  cpmbidprice         ,
  conversion          ,
  allconversion       ,
  revenue             ,
  realrevenue         ,
  l_time              ,
  b_date              ,
  publisheramid       ,
  publisheramname     ,
  advertiseramid      ,
  advertiseramname    ,
  appmodeid           ,
  appmodename         ,
  adcategory1id       ,
  adcategory1name     ,
  campaignname        ,
  adverid             ,
  advername           ,
  offeroptstatus      ,
  offername           ,
  publishername       ,
  appname             ,
  countryname         ,
  carriername         ,
  adtypeid            ,
  adtypename          ,
  versionid           ,
  versionname         ,
  publisherproxyid    ,
  data_type           ,
  feecpctimes         ,
  feecpmtimes         ,
  feecpatimes         ,
  feecpasendtimes     ,
  feecpcreportprice   ,
  feecpmreportprice   ,
  feecpareportprice   ,
  feecpcsendprice     ,
  feecpmsendprice     ,
  feecpasendprice     ,
  countrycode         ,
-- packagename         ,
--  domain              ,
--  operatingsystem     ,
--  systemlanguage      ,
--  devicebrand         ,
--  devicetype          ,
--  browserkernel       ,
  b_time              ,
  companyid           ,
  companyname         ,
  publisherampaid     ,
  publisherampaname   ,
  advertiseramaaid    ,
  advertiseramaaname
from ssp_report_overall_dm_day_v2
where versionname <> "third-income";