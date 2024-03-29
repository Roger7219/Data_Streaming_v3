CREATE TABLE `ADVERTISER`(
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
  `ips` string)
STORED AS ORC;