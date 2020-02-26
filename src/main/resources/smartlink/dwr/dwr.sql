CREATE TABLE smartlink_dwr(
  publisherid       int,
  appid             int,
  countryid         int,
  carrierid         int,
  versionname       string,
  adtype            int,
  campaignid        int,
  offerid           int,
  imageid           int,
  affsub            string,
  requestcount      bigint,
  sendcount         bigint,
  showcount         bigint,
  clickcount        bigint,
  feereportcount    bigint,
  feesendcount      bigint,
  feereportprice    decimal(19,10),
  feesendprice      decimal(19,10),
  cpcbidprice       decimal(19,10),
  cpmbidprice       decimal(19,10),
  conversion        bigint,
  allconversion     bigint,
  revenue           decimal(19,10),
  realrevenue       decimal(19,10),
  feecpctimes       bigint,
  feecpmtimes       bigint,
  feecpatimes       bigint,
  feecpasendtimes   bigint,
  feecpcreportprice decimal(19,10), 
  feecpmreportprice decimal(19,10), 
  feecpareportprice decimal(19,10), 
  feecpcsendprice   decimal(19,10),
  feecpmsendprice   decimal(19,10),
  feecpasendprice   decimal(19,10),
  packagename       string,
  domain            string,
  operatingsystem   string,
  systemlanguage    string,
  devicebrand       string,
  devicetype        string,
  browserkernel     string,
  respstatus        int,
  winprice          double,
  winnotices        bigint,
  test              int,
  ruleid            int,
  smartid           int,
  newcount          bigint,
  activecount       bigint,
  eventname         string,
  ratertype         int,
  raterid           string)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;