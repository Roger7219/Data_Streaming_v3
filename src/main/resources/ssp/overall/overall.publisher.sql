-- alert table ssp_report_publisher_dm rename to ssp_report_publisher_dm__back

drop view if exists ssp_report_publisher_dm;
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
  clickcount + thirdclickcount as clickcount,
  feereportcount     ,
  feesendcount       ,
  feereportprice     ,
  feesendprice       ,
  cpcbidprice        ,
  cpmbidprice        ,
  conversion         ,
  allconversion      ,
  revenue + thirdSendFee as revenue     ,
  realrevenue + thirdFee as realrevenue ,
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
from ssp_report_overall_dm_day;


--  select sum(newcount) from  ssp_report_overall_dm_day  where b_date ="2018-05-01";
--  select b_date, sum(newcount) from  ssp_report_publisher_dm  where b_date >= "2018-04-20"  group by b_date order by 1;



-- 检查数据
select
b_date,
sum(revenue) ,
sum(thirdSendFee),
sum(revenue + thirdSendFee ) as revenue_ALL,
sum(realrevenue),
sum(thirdFee),
sum(realrevenue + thirdFee) AS realrevenue_ALL
FROM ssp_report_overall_dm_day WHERE b_date >="2018-06-05"
group by b_date
ORDER BY b_date


select statdate, sum(thirdFee), sum(thirdSendFee)
from ssp_report_publisher_third_income
where statdate>="2018-05-01"
group by statdate
order by statdate







select b_time, SUM( revenue), SUM(realrevenue)
FROM ssp_report_overall_dwr WHERE b_date ="2018-05-07"
group by b_time
order by b_time




select
b_date,
sum(revenue) ,
sum(thirdSendFee),
sum(revenue + thirdSendFee ) as revenue_ALL,
sum(realrevenue),
sum(thirdFee),
sum(realrevenue + thirdFee) AS realrevenue_ALL
FROM ssp_report_overall_dm_day WHERE b_date ="2018-06-08"
--and appid=38
group by b_date
ORDER BY b_date


select
b_date,
sum(revenue) ,
sum(thirdSendFee),
sum(revenue + thirdSendFee ) as revenue_ALL,
sum(realrevenue),
sum(thirdFee),
sum(realrevenue + thirdFee) AS realrevenue_ALL
FROM report_dataset.ssp_report_publisher_dm WHERE _PARTITIONTIME="2018-06-05" and b_date >="2018-06-05"
and appid=38
group by b_date
ORDER BY b_date