create view ssp_report_publisher_dm as
select
  publisherid        ,
  appid              ,
  countryid          ,
  carrierid          ,
  affsub             ,
  requestcount       ,
  sendcount          ,
  showcount          ,
  clickcount as clickcount,
  feereportcount     ,
  feesendcount       ,
  feereportprice     ,
  feesendprice       ,
  cpcbidprice        ,
  cpmbidprice        ,
  conversion         ,
  allconversion      ,
  revenue  as revenue     ,
  realrevenue as realrevenue ,
  newcount           ,
  activecount        ,
  l_time             ,
  b_date             ,
  publisheramid      ,
  publisheramname    ,
  appmodeid          ,
  appmodename        ,
  publishername      ,
  appname            ,
  countryname        ,
  carriername        ,
  versionid          ,
  versionname        ,
  publisherproxyid   ,
  countrycode        ,
--  packagename        ,
--  domain             ,
--  operatingsystem    ,
--  systemlanguage     ,
--  devicebrand        ,
--  devicetype         ,
--  browserkernel      ,
  b_time             ,
  companyid          ,
  companyname        ,
  publisherampaid    ,
  publisherampaname
from ssp_report_overall_dm_day_v2;
