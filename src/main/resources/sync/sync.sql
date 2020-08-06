CREATE TABLE OFFER_WARNING_CONFIG (
  ecpc double,   -- COMMENT '默认为2.0',
  cr double      -- COMMENT '默认为1%'
)
STORED AS ORC;


-- Hive
 CREATE TABLE ADVERTISER(
   id             int,
   name           string,
   countryid      int,
   phone          string,
   company        string,
   street         string,
   city           string,
   state          string,
   zip            string,
   skype          string,
   qq             string,
   desc           string,
   balance        double,
   todaycost      double,
   monthcost      double,
   totalcost      double,
   isinner        int,
   isapi          int,
   url            string,
   postvalue      string,
   implementclass string,                             
   amid           int,
   adid           int,
   iscpa          int,
   Token          string,
   uniqueId       string,
   MediaBuyId     int,
   OpType         int,
   SubIds         string,
   AmaaId         int
)
STORED AS ORC;

CREATE TABLE EMPLOYEE(                               
    id            int,
    name          string,
    phone         string,
    skype         string,
    qq            string,
    jobtitle      string,
    proxyid       int,
    uniqueId      string,
    companyId     int
)
STORED AS ORC;

CREATE TABLE ADVERTISER_AM(
    id            int,
    name          string,
    phone         string,
    skype         string,
    qq            string,
    jobtitle      string,
    proxyid       int,
    uniqueId      string,
    companyId     int
)
STORED AS ORC;

CREATE TABLE PROXY(
    id            int,
    name          string,
    desc          string,
    key           string,
    uniqueId      string,
    companyId     int
)
STORED AS ORC;

CREATE TABLE COMPANY (
    id            int,
    name          string,
    desc          string
)
STORED AS ORC;

--
-- ShowCount, 'ClickCount, 'DailyCost, 'TotalCost

-- alter table CAMPAIGN add columns(pricemethod    int);
CREATE TABLE CAMPAIGN(
  id             int,
  name          string,
  adcategory1   int,
  adcategory2   int,
  adverId       int,
  totalcost     double,
  dailycost     double,
  showcount     int,
  clickcount    int,
  offercount    int,
  status        int,
  pricemethod   int
)
STORED AS ORC;

-- CREATE TABLE CAMPAIGN(
--     id            int,
--     name          string,
--     adverid       int,
--     status        int,
--     adcategory1   int,
--     adcategory2   int,
--     budget        double,
--     totalbudget   double,
--     pricemethod   int,
--     times         string,
--     createtime    string,
--     totalcost     double,
--     dailycost     double,
--     showcount     int,
--     clickcount    int,
--     offercount    int
-- )
-- STORED AS ORC;

-- alter table offer add columns(countryIds string)
-- alter table offer add columns(countryIds string)
-- alter table offer add columns(createTime string)
-- alter table offer add columns(modes string)
-- alter table offer add columns(carrierids string)
-- alter table offer add columns(bidPrice   double)
-- alter table offer add columns(price   double)
-- alter table offer add columns(type    int)
-- alter table offer add columns(todayshowcount     int)
-- alter table offer add columns(todayclickcount     int)
-- alter table offer add columns(todayfee     double)
-- alter table offer add columns(todaycaps     int)
--
CREATE TABLE OFFER(
  id         int,
  name       string,
  campaignId int,
  optStatus  int,
  countryIds string,
  createTime string,
  modes      string,
  carrierids string,
  bidPrice   double,
  price      double,
  type       int,
  imageurl   string,
  isapi      int,
  status     int,

  todayshowcount int,
  todayclickcount int,
  todayfee double,
  todaycaps int
)
STORED AS ORC;

-- CREATE TABLE OFFER(
--     id                int,
--     name              string,
--     campaignid        int,
--     countryids        string,
--     carrierids        string,
--     bidprice          double,
--     price             double,
--     type              int,
--     imageurl          string,
--     url               string,
--     status            int,
--     amstatus          int,
--     desc              string,
--     optype            int,
--     subids            string,
--     devicetype        string,
--     createtime        string,
--     clickcount        int,
--     todayclickcount   int,
--     todayfee          double,
--     adofferid         int,
--     addesc            string,
--     iscollect         int,
--     regulars          string,
--     collectpercent    int,
--     istest            int,
--     pricepercent      int,
--     caps              int,
--     todaycaps         int,
--     salepercent       int,
--     lpurl             string,
--     adtype            string,
--     optstatus         int,
--     opttime           string,
--     isapi             int,
--     preview           string,
--     showtimes         int,
--     imageurls         string,
--     todayshowcount    int,
--     iconurl           string,
--     spystatus         int,
--     imagelib          int,
--     denyreason        string,
--     modes             string,
--     applytime         string,
--     analysisid        int,
--     optfailreason     int,
--     optresulttime     string,
--     conversionprocess int,
--     level             int,
--     isApiTime         string,
--     isRecommend       int,
--     closeCollectionOperator string,
--     cr                int,
--     uniqueId          string,
--     showCount         int,
--     imgMode           int,
--     testWins          int,
--     modeCaps          string,
--     todayModeCaps     string,
--     needParams        string
-- )
-- STORED AS ORC;

CREATE TABLE PUBLISHER(                              
    id                int,
    name              string,
    countryid         int,
    phone             string,
    company           string,
    street            string,
    city              string,
    state             string,
    zip               string,
    skype             string,
    qq                string,
    desc              string,
    todayrevenue      double,
    monthrevenue      double,
    postback          string,
    amid              int,
    todaysendrevenue  double,
    monthsendrevenue  double,
    pricepercent      int,
    token             string,
    ApiConversionPercent    int,
    ApiPricePercent         int,
    UniqueId                string,
    OtherSmartlinkCounts    int,
    IsOrderByBidPrice       int,
    AmpaId                  int
)
STORED AS ORC;

drop table APP;
CREATE TABLE APP(
    id                int,
    name              string,
    publisherid       int,
    platform          int,
    url               string,
    type              int,
    status            int,
    adtype            string,
    iscpm             int,
    cpclimit          double,
    postback          string,
    createtime        string,
    position          int,
    timestep          int,
    showtimes         int,
    showpercent       int,
    clickpercent      int,
    sendpercent       int,
    pricepercent      int,
    mode              int,
    offerid           int,
    domain            string,
    opentype          int,
    ads               string,
    token             string,
    packagename       string,
    storeurl          string,
    log               int,
    balancepercent    int,
    width             int,
    height            int,
    keys              string,
    commercialtime    string,
    iscollect         int,
    positions         string,
    description       string,
    salepercent       int,
    apiofferids       string,
    optype            int,
    levels            string,
    adveroptype       int,
    adverids          string,
    couoptype         int,
    countryids        string,
    dsps              string,
    dspoptype         int,
    iab1              string,
    iab2              string,
    qualitylevel      int,
    isredirect        int,
    redirectoffer     int,
    redirectincome    int,
    bidprobability    double,
    dailylimitofwins  int,
    totallimitofwins  int,
    dailybudget       double,
    totalbudget       double,
    initialbidprice   double,
    maxbidprice       double,
    minbidprice       double,
    apiconfig         string,
    uniqueId          string,
    IsSecondHighPriceWin int,
    IsOrderByBidPrice   int
)
STORED AS ORC;

CREATE TABLE COUNTRY(
    id                int,
    name              string,
    alpha2_code       string,
    alpha3_code       string,
    numeric_code      string
)
STORED AS ORC;

CREATE TABLE CARRIER(                                
    id                int,                                            
    name              string
)
STORED AS ORC;

CREATE TABLE IMAGE_INFO(                              
    id                int,
    adcategory1       int,
    adcategory2       int,
    size              string,
    countryids        string,
    imageurl          string,
    showcount         int,
    todayshowcount    int,
    todayclickcount   int,
    status            int,
    createtime        string,
    islibrary         int
)
STORED AS ORC;

CREATE TABLE VERSION_CONTROL(                              
    id                int,
    version           string,
    type              int,
    info              string,
    createtime        string,
    updatetime        string
)
STORED AS ORC;

CREATE TABLE JAR_CONFIG(                              
    id                int,
    name              string,
    jarcustomerid     int,
    customerversion   string,
    definedversion    int,
    md5               string,
    downloadurl       string,
    createtime        string,
    encryptionfactor  string,
    description       string,
    optype            int,
    appids            string,
    countryids        string,
    carrierids        string,
    status            int,
    paramsvalue       string,
    paramstype        string,
    startclass        string
)
STORED AS ORC;

CREATE TABLE JAR_CUSTOMER(
   id                 int,
   name               string,
   description        string,
   createtime         string,
   percent          int
)
STORED AS ORC;

CREATE TABLE OFFER(
    id                int,
    name              string,
    campaignid        int,
    countryids        string,
    carrierids        string,
    bidprice          double,
    price             double,
    type              int,
    imageurl          string,
    url               string,
    status            int,
    amstatus          int,
    desc              string,
    optype            int,
    subids            string,
    devicetype        string,
    createtime        string,
    clickcount        int,
    todayclickcount   int,
    todayfee          double,
    adofferid         int,
    addesc            string,
    iscollect         int,
    regulars          string,
    collectpercent    int,
    istest            int,
    pricepercent      int,
    caps              int,
    todaycaps         int,
    salepercent       int,
    lpurl             string,
    adtype            string,
    optstatus         int,
    opttime           string,
    isapi             int,
    preview           string,
    showtimes         int,
    imageurls         string,
    todayshowcount    int,
    iconurl           string,
    spystatus         int,
    imagelib          int,
    denyreason        string,
    modes             string,
    applytime         string,
    analysisid        int,
    optfailreason     int,
    optresulttime     string,
    conversionprocess int,
    level             int
)
STORED AS ORC;

CREATE TABLE APP(
    id             int,
    name           string,
    publisherid    int,
    platform       int,
    url            string,
    type           int,
    status         int,
    adtype         string,
    iscpm          int,
    cpclimit       double,
    postback       string,
    createtime     string,
    position       int,
    timestep       int,
    showtimes      int,
    showpercent    int,
    clickpercent   int,
    sendpercent    int,
    pricepercent   int,
    mode           int,
    offerid        int,
    domain         string,
    opentype       int,
    ads            string,
    token          string,
    packagename    string,
    storeurl       string,
    log            int,
    balancepercent int,
    width          int,
    height         int,
    keys           string,
    commercialtime string,
    iscollect      int,
    positions      string,
    description    string,
    salepercent    int,
    apiofferids    string,
    optype         int,
    levels         string,
    adveroptype    int,
    adverids       string,
    couoptype      int,
    countryids     string
)
STORED AS ORC;

drop table if exists OFFER_DEMAND_CONFIG ;
CREATE TABLE OFFER_DEMAND_CONFIG (
      id                    int,
      countryId             int,
      carrierId             int,
      category              int,
      clickCount            int,
      offerCount            int,
      ecpcHigh             decimal(18,5),
      ecpclow              decimal(18,5)
)
STORED AS ORC;

drop table OTHER_SMART_LINK;
CREATE TABLE OTHER_SMART_LINK (
      id                    int,
      name                  string,
      publisherId           int,
      link                  string,
      createTime            string,
      desc                  string,
      uniqueid              string
)
STORED AS ORC;

drop table SMARTLINK_RULES;
CREATE TABLE SMARTLINK_RULES (
      id                    int,
      name                  string,
      type                  int,
      description           string,
      updateTime            string,
      companyId             int,
      weight                int,
      percents              float,
      url                   string,
      countries             string,
      carriers              string,
      queryParams           string,
      userAgent             string,
      advertiser            string,
      offerLevel            string,
      offerIds              string,
      offerCategorys        string,
      autoOpt               int,
      koklinkId             int,
      uniqueid              string
) 
STORED AS ORC;

drop view if exists SSP_REPORT_PUBLISHER_DM ;
CREATE VIEW SSP_REPORT_PUBLISHER_DM
(   publisherid,
    appid,
    countryid,
    carrierid,
    affsub,
    requestcount,
    sendcount,
    showcount,
    clickcount,
    feereportcount,
    feesendcount,
    feereportprice,
    feesendprice,
    cpcbidprice,
    cpmbidprice,
    conversion,
    allconversion,
    revenue,
    realrevenue,
    newcount,
    activecount,
    l_time,
    b_date,
    publisheramid,
    publisheramname,
    appmodeid,
    appmodename,
    publishername,
    appname,
    countryname,
    carriername,
    versionid,
    versionname,
    publisherproxyid
) AS
select
    nvl(dwr.publisherid, a.publisherid) as publisherid ,-- INT, ???join????app????????????????????join??
    dwr.appid          ,-- INT,
    dwr.countryid      ,-- INT,
    dwr.carrierid      ,-- INT,
    dwr.affsub         ,-- STRING,
    dwr.requestcount   ,-- BIGINT,
    dwr.sendcount      ,-- BIGINT,
    dwr.showcount      ,-- BIGINT,
    dwr.clickcount     ,-- BIGINT,
    dwr.feereportcount ,-- BIGINT,         -- ????
    dwr.feesendcount   ,-- BIGINT,         -- ??????
    dwr.feereportprice ,-- DECIMAL(19,10), -- ????(????)
    dwr.feesendprice   ,-- DECIMAL(19,10), -- ??????(??)
    dwr.cpcbidprice    ,-- DECIMAL(19,10),
    dwr.cpmbidprice    ,-- DECIMAL(19,10),
    dwr.conversion     ,-- BIGINT,         -- ?????????????????
    dwr.allconversion  ,-- BIGINT,         -- ?????????????
    dwr.revenue        ,-- DECIMAL(19,10), -- ??
    dwr.realrevenue    ,-- DECIMAL(19,10), -- ????
    dwr.newcount       ,-- BIGINT,
    dwr.activecount    ,-- BIGINT
    dwr.l_time,
    dwr.b_date,

    p.amid              as publisheramid,
    e.name              as publisheramname,
    a.mode              as appmodeid,
    m.name              as appmodename,
    p.name              as publishername,
    a.name              as appname,
    c.name              as countryname,
    ca.name             as carriername,
    v.id                as versionid,
    dwr.versionname     as versionname,
    e.proxyid           as publisherproxyid
from default.ssp_report_publisher_dwr dwr
left join default.publisher p       on p.id = dwr.publisherid
LEFT JOIN default.employee e        on e.id = p.amid
left join default.app a             on a.id = dwr.appid
left join default.app_mode m        on m.id = a.mode
left join default.country c         on c.id = dwr.countryid
left join default.carrier ca        on ca.id = dwr.carrierid
left join default.version_control v on v.version = dwr.versionname
where v.id is not null
union all
select
    -1 as publisherid   ,
    t.appid as appid    ,
    t.countryid as countryid,
    -1 as carrierid     ,
    'third-income' as affsub,
    0 as requestcount   ,
    0 as sendcount      ,
    0 as showcount      ,
    t.pv as clickcount  ,
    0 as feereportcount ,
    0 as feesendcount   ,
    0 as feereportprice ,
    0 as feesendprice   ,
    0 as cpcbidprice    ,
    0 as cpmbidprice    ,
    0 as conversion     ,
    0 as allconversion  ,
    t.thirdsendfee as revenue,
    t.thirdfee     as realrevenue ,
    0 as newcount       ,
    0 as activecount    ,
    '2000-01-01 00:00:00' as l_time,
    t.statdate as b_date  ,

    -1 as publisheramid ,
    'third-income' as publisheramname ,
    -1 as appmodeid     ,
    'third-income' as appmodename ,
    'third-income' as publishername ,

    a.name          as appname ,
    c.name          as countryname ,
    'third-income'  as carriername ,
    -1              as versionid ,
    'third-income'  as versionname ,
    -1 as publisherproxyid
from default.ssp_report_publisher_third_income t
left join default.app a             on a.id = t.appid
left join default.country c         on c.id = t.countryid


-- Greenplum
drop table ADVERTISER;
CREATE TABLE ADVERTISER (
  ID             int,
  Name           varchar(100),
  CountryId      int,
  Phone          varchar(20),
  Company        varchar(200),
  Street         varchar(100),
  City           varchar(100),
  State          varchar(100),
  Zip            varchar(100),
  Skype          varchar(100),
  QQ             varchar(100),
  "desc"         varchar(500),
  Balance        decimal(18,4),
  TodayCost      decimal(18,4),
  MonthCost      decimal(18,4),
  TotalCost      decimal(18,4),
  IsInner        smallint,
  IsApi          smallint,
  Url            varchar(200),
  PostValue      varchar(200),
  ImplementClass varchar(200),
  AmId           int,
  AdId           int,
  isCpa          smallint,
  token          varchar(255),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='advertiser_default_partition')
);

CREATE TABLE EMPLOYEE (
  ID            int,
  Name          varchar(100),
  Phone         varchar(20),
  Skype         varchar(100),
  QQ            varchar(100),
  JobTitle      varchar(100),
  ProxyId       int,
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='employee_default_partition')
);

CREATE TABLE ADVERTISER_AM (
  ID            int,
  Name          varchar(100),
  Phone         varchar(20),
  Skype         varchar(100),
  QQ            varchar(100),
  JobTitle      varchar(100),
  ProxyId       int,
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='advertiser_am_default_partition')
);

CREATE TABLE CAMPAIGN (
  ID            int,
  Name          varchar(100),
  AdverId       int,
  Status        int,
  AdCategory1   int,
  AdCategory2   int,
  Budget        decimal(18,4),
  TotalBudget   decimal(18,4),
  PriceMethod   int,
  Times         varchar(100),
  CreateTime    varchar(100),
  TotalCost     decimal(18,4),
  DailyCost     decimal(18,4),
  ShowCount     int,
  ClickCount    int,
  OfferCount    int,
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='campaign_default_partition')
);

drop table OFFER;
CREATE TABLE OFFER (
  ID                int,
  Name              varchar(100),
  CampaignId        int,
  CountryIds        varchar(1000),
  CarrierIds        varchar(2000),
  BidPrice          decimal(18,4),
  Price             decimal(18,4),
  Type              int,
  ImageUrl          varchar(100),
  Url               varchar(1000),
  Status            int,
  AmStatus          int,
  "desc"            text,
  OpType            int,
  SubIds            varchar(1000),
  DeviceType        varchar(200),
  CreateTime        varchar(100),
  ClickCount        int,
  TodayClickCount   int,
  TodayFee          decimal(18,4),
  AdOfferId         int,
  AdDesc            text,
  IsCollect         int,
  regulars          text,
  CollectPercent    int,
  IsTest            int,
  PricePercent      int,
  Caps              int,
  TodayCaps         int,
  SalePercent       int,
  LpUrl             varchar(1000),
  AdType            varchar(1000),
  OptStatus         int,
  OptTime           varchar(100),
  IsApi             int,
  Preview           varchar(1000),
  ShowTimes         int,
  ImageUrls         varchar(5000),
  TodayShowCount    int,
  IconUrl           varchar(1000),
  SpyStatus         int,
  ImageLib          int,
  DenyReason        varchar(2000),
  Modes             varchar(200),
  ApplyTime         varchar(100),
  AnalysisId        int,
  OptFailReason     int,
  OptResultTime     varchar(100),
  ConversionProcess int,
  level             int,
  IsApiTime         varchar(100),
  IsRecommend       int,
  CloseCollectionOperator varchar(255),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='offer_default_partition')
);

CREATE TABLE PUBLISHER (
  ID                int,
  Name              varchar(100),
  CountryId         int,
  Phone             varchar(20),
  Company           varchar(200),
  Street            varchar(100),
  City              varchar(100),
  State             varchar(100),
  Zip               varchar(100),
  Skype             varchar(100),
  QQ                varchar(100),
  "desc"            varchar(500),
  TodayRevenue      decimal(18,5),
  MonthRevenue      decimal(18,5),
  PostBack          varchar(1000),
  amId              int,
  TodaySendRevenue  decimal(18,5),
  MonthSendRevenue  decimal(18,5),
  PricePercent      int,
  Token             varchar(255),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='publisher_default_partition')
);

CREATE TABLE APP (
  ID                int,
  Name              varchar(100),
  PublisherId       int,
  Platform          int,
  Url               varchar(250),
  Type              int,
  Status            int,
  AdType            varchar(1000),
  isCpm             int,
  cpcLimit          decimal(18,4),
  PostBack          varchar(500),
  CreateTime        varchar(100),
  Position          int,
  TimeStep          int,
  ShowTimes         int,
  ShowPercent       int,
  ClickPercent      int,
  SendPercent       int,
  PricePercent      int,
  Mode              int,
  OfferId           int,
  Domain            varchar(100),
  OpenType          int,
  Ads               varchar(1000),
  Token             varchar(1000),
  PackageName       varchar(1000),
  StoreUrl          varchar(1000),
  "log"             int,
  BalancePercent    int,
  width             int,
  height            int,
  Keys              varchar(5000),
  CommercialTime    varchar(500),
  IsCollect         int,
  Positions         varchar(1000),
  Description       varchar(1000),
  SalePercent       int,
  ApiOfferIds       varchar(2000),
  OpType            int,
  levels            varchar(100),
  AdverOpType       int,
  AdverIds          varchar(1000),
  CouOpType         int,
  CountryIds        varchar(1000),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='app_default_partition')
);

CREATE TABLE COUNTRY (
  ID                int,
  Name              varchar(100),
  ALPHA2_CODE       varchar(2),
  ALPHA3_CODE       varchar(3),
  NUMERIC_CODE      varchar(10),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='country_default_partition')
);

CREATE TABLE CARRIER (
  ID                int,
  Name              varchar(100),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='carrier_default_partition')
);

CREATE TABLE IMAGE_INFO (
  ID                int,
  AdCategory1       int,
  AdCategory2       int,
  Size              varchar(100),
  CountryIds        varchar(500),
  ImageUrl          varchar(1000),
  ShowCount         int,
  TodayShowCount    int,
  TodayClickCount   int,
  Status            int,
  CreateTime        varchar(100),
  IsLibrary         int,
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='image_info_default_partition')
);

CREATE TABLE VERSION_CONTROL (
  ID                int,
  Version           varchar(100),
  Type              int,
  Info              text,
  CreateTime        varchar(100),
  UpdateTime        varchar(100),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='version_control_default_partition')
);

CREATE TABLE JAR_CONFIG (
  ID                int,
  Name              varchar(255),
  JarCustomerId     int,
  CustomerVersion   varchar(255),
  DefinedVersion    int,
  Md5               varchar(255),
  DownloadUrl       varchar(255),
  CreateTime        varchar(100),
  EncryptionFactor  varchar(255),
  Description       varchar(255),
  OpType            int,
  appIds            varchar(2000),
  CountryIds        varchar(2000),
  CarrierIds        varchar(2000),
  Status            int,
  paramsValue       varchar(1000),
  paramsType        varchar(1000),
  startClass        varchar(1000),
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='jar_config_default_partition')
);

CREATE TABLE JAR_CUSTOMER (
  id                int,
  name              varchar(255),
  description       varchar(1000),
  createTime        varchar(100),
  percent           INT,
  exchange_partition varchar(100)
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='jar_customer_default_partition')
);
  
=======


------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_publisher_totalcost_dwr_view;
create view ssp_publisher_totalcost_dwr_view as
select
    publisherId,
    sum(monthSendRevenue) as monthSendRevenue,
    sum(monthRevenue) as monthRevenue,
    l_time,
    b_date
from (
    select
        publisherId,
        sum(sendPrice)     as monthSendRevenue,
        sum(reportPrice)   as monthRevenue,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01') as b_date
    from ssp_fee_dwr
    where second(l_time) = 0
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')
    UNION ALL
    select
       publisherId,
       sum(cpcSendPrice)   as monthSendRevenue,
       sum(cpcBidPrice)    as monthRevenue,
       date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
       date_format(b_date, 'yyyy-MM-01')
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')
    UNION ALL
    select
        publisherId,
        sum(cpmSendPrice) as monthSendRevenue,
        sum(cpmBidPrice)  as monthRevenue,
        date_format(l_time, 'yyyy-MM-01 00:00:ss') as l_time,
        date_format(b_date, 'yyyy-MM-01')
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-01 00:00:ss'),
        date_format(b_date, 'yyyy-MM-01')
)t0
group by
    publisherId,
    l_time,
    b_date;


--CREATE TABLE IF NOT EXISTS ssp_publisher_totalcost_dwr_tmp like ssp_publisher_totalcost_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_publisher_totalcost_dwr
select * from ssp_publisher_totalcost_dwr_view;
--where b_date <'2017-09-10';



drop view if exists ssp_publisher_daily_dwr_view;
create view ssp_publisher_daily_dwr_view as
select
    publisherId,
    sum(todaySendRevenue) as todaySendRevenue,
    sum(todayRevenue)     as todayRevenue,
    l_time,
    b_date
from (
    select
        publisherId,
        sum(sendPrice)     as todaySendRevenue,
        sum(reportPrice)   as todayRevenue,
        date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time,
        b_date
    from ssp_fee_dwr
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-dd 00:00:00'),
        b_date
    UNION ALL
    select
       publisherId,
       sum(cpcSendPrice)   as todaySendRevenue,
       sum(cpcBidPrice)    as todayRevenue,
       date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time,
       b_date
    from ssp_click_dwr
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-dd 00:00:00'),
        b_date
    UNION ALL
    select
        publisherId,
        sum(cpmSendPrice) as todaySendRevenue,
        sum(cpmBidPrice)  as todayRevenue,
        date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time,
        b_date
    from ssp_show_dwr
    group by
        publisherId,
        date_format(l_time, 'yyyy-MM-dd 00:00:00'),
        b_date
)t0
group by
    publisherId,
    l_time,
    b_date;


--CREATE TABLE IF NOT EXISTS ssp_publisher_daily_dwr_tmp like ssp_publisher_daily_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_publisher_daily_dwr
select * from ssp_publisher_totalcost_dwr_view;

--where b_date <'2017-09-10';
