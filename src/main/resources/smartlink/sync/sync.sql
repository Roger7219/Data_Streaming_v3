CREATE TABLE `advertiser`(
  `id` int,
  `name` string,
  `countryid` int,
  `phone` string,
  `company` string,
  `street` string,
  `city` string,
  `state` string,
  `zip` string,
  `skype` string,
  `qq` string,
  `desc` string,
  `balance` decimal(18,4),
  `todaycost` decimal(18,4),
  `monthcost` decimal(18,4),
  `totalcost` decimal(18,4),
  `isinner` int,
  `isapi` int,
  `url` string,
  `postvalue` string,
  `implementclass` string,
  `amid` int,
  `adid` int,
  `iscpa` int,
  `token` string,
  `uniqueid` string,
  `mediabuyid` int,
  `optype` int,
  `subids` string,
  `amaaid` int,
  `status` int,
  `blockapi` int,
  `ips` string
)
STORED AS ORC;


CREATE TABLE `employee`(
  `id` int,
  `name` string,
  `phone` string,
  `skype` string,
  `qq` string,
  `jobtitle` string,
  `proxyid` int,
  `uniqueid` string,
  `companyid` int
)STORED AS ORC;

CREATE TABLE `advertiser_am`(
  `id` int,
  `name` string,
  `phone` string,
  `skype` string,
  `qq` string,
  `jobtitle` string,
  `proxyid` int,
  `uniqueid` string,
  `companyid` int
)STORED AS ORC;

CREATE TABLE `campaign`(
  `id` int,
  `name` string,
  `adverid` int,
  `status` int,
  `adcategory1` int,
  `adcategory2` int,
  `budget` decimal(18,4),
  `totalbudget` decimal(18,4),
  `pricemethod` int,
  `times` string,
  `createtime` timestamp,
  `totalcost` decimal(18,4),
  `dailycost` decimal(18,4),
  `showcount` int,
  `clickcount` int,
  `offercount` int,
  `uniqueid` string,
  `caps` int,
  `todaycaps` int,
  `optype` int,
  `subids` string,
  `endtime` timestamp,
  `min_os_version` string,
  `trafficmap` string
)STORED AS ORC;

CREATE TABLE `offer`(
  `id` int,
  `name` string,
  `campaignid` int,
  `countryids` string,
  `carrierids` string,
  `bidprice` decimal(18,4),
  `price` decimal(18,4),
  `type` int,
  `imageurl` string,
  `url` string,
  `status` int,
  `amstatus` int,
  `desc` string,
  `optype` int,
  `subids` string,
  `devicetype` string,
  `createtime` timestamp,
  `clickcount` int,
  `todayclickcount` int,
  `todayfee` decimal(18,4),
  `adofferid` int,
  `addesc` string,
  `iscollect` int,
  `regulars` string,
  `collectpercent` int,
  `istest` int,
  `pricepercent` int,
  `caps` int,
  `todaycaps` int,
  `salepercent` int,
  `lpurl` string,
  `adtype` string,
  `optstatus` int,
  `opttime` timestamp,
  `isapi` int,
  `preview` string,
  `showtimes` int,
  `imageurls` string,
  `todayshowcount` int,
  `iconurl` string,
  `spystatus` int,
  `imagelib` int,
  `denyreason` string,
  `modes` string,
  `applytime` timestamp,
  `analysisid` int,
  `optfailreason` int,
  `optresulttime` timestamp,
  `conversionprocess` int,
  `level` int,
  `isapitime` timestamp,
  `isrecommend` int,
  `closecollectionoperator` string,
  `cr` int,
  `uniqueid` string,
  `showcount` int,
  `imgmode` int,
  `testwins` int,
  `modecaps` string,
  `todaymodecaps` string,
  `needparams` string,
  `publisherpayout` decimal(18,4),
  `mediabuyids` string,
  `oldcpaofferid` int,
  `regions` string,
  `citys` string,
  `endtime` timestamp,
  `imageszipurl` string,
  `appgroupconfig` string,
  `packagename` string,
  `isspypause` int,
  `redirectpercent` int,
  `redirectoffer` int,
  `smartclickcount` int,
  `totalsmartclickcount` int,
  `appcaps` string,
  `smartpricepercent` int,
  `smartsalepercent` int,
  `redirectcount` int,
  `logo` string,
  `creatives` string,
  `recommendapplist` string,
  `recommendapplistopt` int
)STORED AS ORC;

CREATE TABLE `publisher`(
  `id` int,
  `name` string,
  `countryid` int,
  `phone` string,
  `company` string,
  `street` string,
  `city` string,
  `state` string,
  `zip` string,
  `skype` string,
  `qq` string,
  `desc` string,
  `todayrevenue` double,
  `monthrevenue` double,
  `postback` string,
  `amid` int,
  `todaysendrevenue` double,
  `monthsendrevenue` double,
  `pricepercent` int,
  `token` string,
  `apiconversionpercent` int,
  `apipricepercent` int,
  `uniqueid` string,
  `othersmartlinkcounts` int,
  `isorderbybidprice` int,
  `ampaid` int
)STORED AS ORC;

CREATE TABLE `app`(
  `id` int,
  `name` string,
  `publisherid` int,
  `platform` int,
  `url` string,
  `type` int,
  `status` int,
  `adtype` string,
  `iscpm` int,
  `cpclimit` decimal(18,4),
  `postback` string,
  `createtime` timestamp,
  `position` int,
  `timestep` int,
  `showtimes` int,
  `showpercent` int,
  `clickpercent` int,
  `sendpercent` int,
  `pricepercent` int,
  `mode` int,
  `offerid` int,
  `domain` string,
  `opentype` int,
  `ads` string,
  `token` string,
  `packagename` string,
  `storeurl` string,
  `log` int,
  `balancepercent` int,
  `width` int,
  `height` int,
  `keys` string,
  `commercialtime` timestamp,
  `iscollect` int,
  `positions` string,
  `description` string,
  `salepercent` int,
  `apiofferids` string,
  `optype` int,
  `levels` string,
  `adveroptype` int,
  `adverids` string,
  `couoptype` int,
  `countryids` string,
  `dsps` string,
  `dspoptype` int,
  `iab1` string,
  `iab2` string,
  `qualitylevel` int,
  `isredirect` int,
  `redirectoffer` int,
  `redirectincome` int,
  `bidprobability` decimal(18,4),
  `dailylimitofwins` int,
  `totallimitofwins` int,
  `dailybudget` decimal(18,4),
  `totalbudget` decimal(18,4),
  `initialbidprice` decimal(18,4),
  `maxbidprice` decimal(18,4),
  `minbidprice` decimal(18,4),
  `apiconfig` string,
  `uniqueid` string,
  `issecondhighpricewin` int,
  `isorderbybidprice` int,
  `smartconfig` string,
  `mediabuyid` int,
  `apiofferconfig` string
)STORED AS ORC;

CREATE TABLE `country`(
  `id` int,
  `name` string,
  `alpha2_code` string,
  `alpha3_code` string,
  `numeric_code` string
)STORED AS ORC;


CREATE TABLE `carrier`(
  `id` int,
  `name` string
)STORED AS ORC;

CREATE TABLE `image_info`(
  `id` int,
  `adcategory1` int,
  `adcategory2` int,
  `size` string,
  `countryids` string,
  `imageurl` string,
  `showcount` int,
  `todayshowcount` int,
  `todayclickcount` int,
  `status` int,
  `createtime` string,
  `islibrary` int,
  `uniqueid` string,
  `companyid` int
)STORED AS ORC;

CREATE TABLE `version_control`(
  `id` int,
  `version` string,
  `type` int,
  `info` string,
  `createtime` string,
  `updatetime` string
)STORED AS ORC;

CREATE TABLE `jar_config`(
  `id` int,
  `name` string,
  `jarcustomerid` int,
  `customerversion` string,
  `definedversion` int,
  `md5` string,
  `downloadurl` string,
  `createtime` string,
  `encryptionfactor` string,
  `description` string,
  `optype` int,
  `appids` string,
  `countryids` string,
  `carrierids` string,
  `status` int,
  `paramsvalue` string,
  `paramstype` string,
  `startclass` string
)STORED AS ORC;

CREATE TABLE `jar_customer`(
  `id` int,
  `name` string,
  `description` string,
  `createtime` string,
  `percent` int
)STORED AS ORC;

CREATE TABLE `dsp_info`(
  `id` int,
  `name` string
)STORED AS ORC;

CREATE TABLE `iab`(
  `iab1` string,
  `iab2` string,
  `iab1name` string,
  `iab2name` string
)STORED AS ORC;

CREATE TABLE `app_ad`(
  `id` int,
  `name` string,
  `appid` int,
  `adtype` int,
  `width` int,
  `height` int,
  `iab1` string,
  `iab2` string,
  `bidprice` double,
  `token` string
)STORED AS ORC;

CREATE TABLE `proxy`(
  `id` int,
  `name` string,
  `desc` string,
  `key` string,
  `uniqueid` string,
  `companyid` int
)STORED AS ORC;

CREATE TABLE `company`(
  `id` int,
  `name` string,
  `desc` string
)STORED AS ORC;

CREATE TABLE `offer_demand_config`(
  `id` int,
  `countryid` int,
  `carrierid` int,
  `category` int,
  `clickcount` int,
  `offercount` int,
  `ecpchigh` decimal(18,5),
  `ecpclow` decimal(18,5)
)STORED AS ORC;

CREATE TABLE `other_smart_link`(
  `id` int,
  `name` string,
  `publisherid` int,
  `link` string,
  `createtime` string,
  `desc` string,
  `uniqueid` string
)STORED AS ORC;

CREATE TABLE `smartlink_rules`(
  `id` int,
  `name` string,
  `type` int,
  `description` string,
  `updatetime` string,
  `companyid` int,
  `weight` int,
  `percents` float,
  `url` string,
  `countries` string,
  `carriers` string,
  `queryparams` string,
  `useragent` string,
  `advertiser` string,
  `offerlevel` string,
  `offerids` string,
  `offercategorys` string,
  `autoopt` int,
  `koklinkid` int,
  `uniqueid` string
)STORED AS ORC;


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

create table ad_type(
   id   INT,
   name STRING
) STORED AS ORC;
INSERT OVERWRITE TABLE ad_type
VALUES
( 2, 'Mobile-320×50'),
( 3, 'Mobile-300×250'),
( 4, 'Interstitial'),
( 5, 'Mobile-320×480'),
( 6, 'Mobile-1200*627'),
( 7, 'AdType-Id-7'),
( 8, 'AdType-Id-8'),
( 9, 'Mobile-600x600'),
( 10, 'Mobile-720x290');

CREATE TABLE ad_category2(id int, name STRING);
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