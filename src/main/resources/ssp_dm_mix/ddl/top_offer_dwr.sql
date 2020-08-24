CREATE TABLE `top_offer_dwr`(
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
  `feecount` decimal(15,4),
  `totalfeecount` decimal(15,4))
PARTITIONED BY (
  `statdate` string)
STORED AS ORC;