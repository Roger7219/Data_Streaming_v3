CREATE TABLE `CAMPAIGN`(
  `id` int,
  `name` string,
  `adcategory1` int,
  `adcategory2` int,
  `adverid` int,
  `totalcost` double,
  `dailycost` double,
  `showcount` int,
  `clickcount` int,
  `offercount` int,
  `status` int,
  `pricemethod` int)
STORED AS ORC;