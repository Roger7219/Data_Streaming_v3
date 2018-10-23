CREATE TABLE IF NOT EXISTS `offer`(
`id` int,
`name` string,
`campaignid` int,
`countryids` string,
`carrierids` string,
`bidprice` double,
`price` double,
`type` int,
`imageurl` string,
`url` string,
`status` int,
`amstatus` int,
`desc` string,
`optype` tinyint,
`subids` string,
`devicetype` string,
`createtime` string,
`clickcount` int,
`todayclickcount` int,
`todayfee` double,
`adofferid` int,
`addesc` string,
`iscollect` tinyint,
`regulars` string,
`collectpercent` int,
`istest` tinyint,
`pricepercent` int,
`caps` int,
`todaycaps` int,
`salepercent` int,
`lpurl` string,
`adtype` string,
`optstatus` int,
`opttime` string,
`isapi` tinyint,
`preview` string,
`showtimes` int,
`imageurls` string,
`todayshowcount` int,
`iconurl` string,
`spystatus` int,
`imagelib` int,
`denyreason` string,
`modes` string,
`applytime` string,
`analysisid` int,
`optfailreason` tinyint,
`optresulttime` string,
`conversionprocess` int,
`level` int)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `advertiser` (
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
	`balance` double,
	`todaycost` double,
	`monthcost` double,
	`totalcost` double,
	`isinner` tinyint,
	`isapi` tinyint,
	`url` string,
	`postvalue` string,
	`implementclass` string,
	`amid` int,
	`adid` int,
	`iscpa` tinyint
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `CAMPAIGN` (
	`id` int,
	`name` string,
	`adverid` int,
	`status` int,
	`adcategory1` int,
	`adcategory2` int,
	`budget` double,
	`totalbudget` double,
	`pricemethod` tinyint,
	`times` string,
	`createtime` string,
	`totalcost` double,
	`dailycost` double,
	`showcount` int,
	`clickcount` int,
	`offercount` int
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `PUBLISHER` (
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
	`token` string
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `APP` (
	`id` int,
	`name` string,
	`publisherid` int,
	`platform` int,
	`url` string,
	`type` int,
	`status` int,
	`adtype` string,
	`iscpm` tinyint,
	`cpclimit` double,
	`postback` string,
	`createtime` string,
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
	`log` tinyint,
	`balancepercent` int,
	`width` int,
	`height` int,
	`keys` string,
	`commercialtime` string,
	`iscollect` tinyint,
	`positions` string,
	`description` string,
	`salepercent` int,
	`apiofferids` string,
	`optype` int,
	`levels` string,
	`adveroptype` tinyint,
	`adverids` string,
	`couoptype` tinyint,
	`countryids` string
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `COUNTRY` (
	`id` int,
	`name` string,
	`alpha2_code` string,
	`alpha3_code` string,
	`numeric_code` string
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `CARRIER` (
	`id` int,
	`name` string
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `VERSION_CONTROL` (
	`id` int,
	`version` string,
	`type` tinyint,
	`info` string,
	`createtime` string,
	`updatetime` string
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `EMPLOYEE` (
	`id` int,
	`name` string,
	`phone` string,
	`skype` string,
	`qq` string,
	`jobtitle` string,
	`proxyid` int
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `ADVERTISER_AM` (
	`id` int,
	`name` string,
	`phone` string,
	`skype` string,
	`qq` string,
	`jobtitle` string,
	`proxyid` int
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS `IMAGE_INFO` (
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
	`islibrary` tinyint
)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS  `JAR_CONFIG` (
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
	`optype` tinyint,
	`appids` string,
	`countryids` string,
	`carrierids` string,
	`status` tinyint,
	`paramsvalue` string,
	`paramstype` string,
	`startclass` string
)
STORED AS ORC;