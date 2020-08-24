-- 原来的ssp系统迁移过来的数据
CREATE TABLE `top_offer`(
  `category` int,
  `type` int,
  `countryid` int,
  `carrierid` int,
  `offerid` int,
  `sendcount` bigint,
  `clickcount` bigint,
  `showcount` bigint,
  `fee` decimal(15,4),
  `totalsendcount` bigint,
  `totalclickcount` bigint,
  `totalshowcount` bigint,
  `totalfee` decimal(15,4),
  `feecount` bigint,
  `totalfeecount` bigint)
PARTITIONED BY (
  `statdate` string)
STORED AS ORC;