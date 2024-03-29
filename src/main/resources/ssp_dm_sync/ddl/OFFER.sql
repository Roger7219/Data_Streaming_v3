CREATE TABLE `OFFER`(
  `id` int,
  `name` string,
  `campaignid` int,
  `optstatus` int,
  `countryids` string,
  `createtime` string,
  `modes` string,
  `carrierids` string,
  `bidprice` double,
  `price` double,
  `type` int,
  `imageurl` string,
  `isapi` int,
  `status` int,
  `todayshowcount` int,
  `todayclickcount` int,
  `todayfee` double,
  `todaycaps` int)
STORED AS ORC;