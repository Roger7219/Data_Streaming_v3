create table ssp_report_old_campaign_dm(
  publisherid          int             ,
  appid                int             ,
  countryid            int             ,
  carrierid            int             ,
  adtype               int             ,
  campaignid           int             ,
  offerid              int             ,
  imageid              int             ,
  affsub               string          ,
  requestcount         bigint          ,
  sendcount            bigint          ,
  showcount            bigint          ,
  clickcount           bigint          ,
  feereportcount       bigint          ,
  feesendcount         bigint          ,
  feereportprice       decimal(30,10)  ,
  feesendprice         decimal(30,10)  ,
  cpcbidprice          decimal(30,10)  ,
  cpmbidprice          decimal(30,10)  ,
  conversion           bigint          ,
  allconversion        bigint          ,
  revenue              decimal(30,10)  ,
  realrevenue          decimal(30,10)  ,
--  l_time               string          ,
--  b_date               string          ,
  publisheramid        int             ,
  publisheramname      string          ,
  advertiseramid       int             ,
  advertiseramname     string          ,
  appmodeid            int             ,
  appmodename          string          ,
  adcategory1id        int             ,
  adcategory1name      string          ,
  campaignname         string          ,
  adverid              int             ,
  advername            string          ,
  offeroptstatus       int             ,
  offername            string          ,
  publishername        string          ,
  appname              string          ,
  countryname          string          ,
  carriername          string          ,
  adtypeid             int             ,
  adtypename           string          ,
  versionid            int             ,
  versionname          string          ,
  publisherproxyid     int             ,
  data_type            string          ,
  feecpctimes          bigint          ,
  feecpmtimes          bigint          ,
  feecpatimes          bigint          ,
  feecpasendtimes      bigint          ,
  feecpcreportprice    decimal(30,10)  ,
  feecpmreportprice    decimal(30,10)  ,
  feecpareportprice    decimal(30,10)  ,
  feecpcsendprice      decimal(30,10)  ,
  feecpmsendprice      decimal(30,10)  ,
  feecpasendprice      decimal(30,10)  ,
  countrycode          string          ,
--  b_time               string          ,
  companyid            int             ,
  companyname          string          ,
  publisherampaid      int             ,
  publisherampaname    string          ,
  advertiseramaaid     int             ,
  advertiseramaaname   string
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;