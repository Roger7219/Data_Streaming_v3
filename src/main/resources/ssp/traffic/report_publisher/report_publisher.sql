create table ssp_report_publisher_dwr(
       publisherId    INT,
       appId          INT,
       countryId      INT,
       carrierId      INT,
       versionName    STRING,         -- eg: v1.2.3
       affSub         STRING,
       requestCount   BIGINT,
       sendCount      BIGINT,
       showCount      BIGINT,
       clickCount     BIGINT,
       feeReportCount BIGINT,         -- 计费条数
       feeSendCount   BIGINT,         -- 计费显示条数
       feeReportPrice DECIMAL(19,10), -- 计费金额(真实收益)
       feeSendPrice   DECIMAL(19,10), -- 计费显示金额(收益)
       cpcBidPrice    DECIMAL(19,10),
       cpmBidPrice    DECIMAL(19,10),
       conversion     BIGINT,         -- 转化数，目前不要含展示和点击产生的
       allConversion  BIGINT,         -- 转化数，含展示和点击产生的
       revenue        DECIMAL(19,10), -- 收益
       realRevenue    DECIMAL(19,10), -- 真实收益
       newCount       BIGINT,
       activeCount    BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/21
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (domain STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_report_publisher_dwr ADD COLUMNS (b_time STRING);


create table ssp_report_publisher_third_income(
       jarCustomerId  INT,
       appId          INT,
       countryId      INT,
       Pv             BIGINT,
       thirdFee       DECIMAL(19,10),
       thirdSendFee   DECIMAL(19,10)
)
PARTITIONED BY (statdate STRING)
STORED AS ORC;

CREATE TABLE PUBLISHER_STAT (
  AppId             int,
  CountryId         int,
  SdkVersion        STRING,
  SendCount         BIGINT,
  ShowCount         BIGINT,
  ClickCount        BIGINT,
  Fee               decimal(15,5),
  FeeCount          BIGINT,
  ShowSendCount     BIGINT,
  ShowShowCount     BIGINT,
  ShowClickCount    BIGINT,
  UserCount         BIGINT,
  ActiveCount       BIGINT,
  SendFee           decimal(15,5),
  SendFeeCount      BIGINT,
  RequestCount      BIGINT,
  ThirdFee          decimal(15,5),
  ThirdSendFee      decimal(15,5),
  ThirdClickCount   BIGINT
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;


-- Greenplum
create table ssp_report_publisher_dm(
    publisherId    INT,
    appId          INT,
    countryId      INT,
    carrierId      INT,
    --versionName    VARCHAR(200),         -- eg: v1.2.3
    affSub         VARCHAR(200),
    requestCount   BIGINT,
    sendCount      BIGINT,
    showCount      BIGINT,
    clickCount     BIGINT,
    feeReportCount BIGINT,         -- 计费条数
    feeSendCount   BIGINT,         -- 计费显示条数
    feeReportPrice DECIMAL(19,10), -- 计费金额(真实收益)
    feeSendPrice   DECIMAL(19,10), -- 计费显示金额(收益)
    cpcBidPrice    DECIMAL(19,10),
    cpmBidPrice    DECIMAL(19,10),
    conversion     BIGINT,         -- 转化数，目前不要含展示和点击产生的
    allConversion  BIGINT,         -- 转化数，含展示和点击产生的
    revenue        DECIMAL(19,10), -- 收益
    realRevenue    DECIMAL(19,10), -- 真实收益
    newCount       BIGINT,
    activeCount    BIGINT,
    l_time         VARCHAR(200),
    b_date         VARCHAR(200),

    publisherAmId     integer,
    publisherAmName   character varying,
    appModeId         integer,
    appModeName       character varying,
    publisherName     character varying,
    appName           character varying(100),
    countryName       character varying(100),
    carrierName       character varying,
    versionId         integer,
    versionName       character varying,
    publisherProxyId  integer
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_report_publisher_dm_default_partition')
);

-- Greenplum
create table ssp_report_publisher_third_income_dm (
    publisherId    INT,
    appId          INT,
    countryId      INT,
    carrierId      INT,
--    versionName    VARCHAR(200),         -- eg: v1.2.3
    affSub         VARCHAR(200),
    requestCount   BIGINT,
    sendCount      BIGINT,
    showCount      BIGINT,
    clickCount     BIGINT,
    feeReportCount BIGINT,         -- 计费条数
    feeSendCount   BIGINT,         -- 计费显示条数
    feeReportPrice DECIMAL(19,10), -- 计费金额(真实收益)
    feeSendPrice   DECIMAL(19,10), -- 计费显示金额(收益)
    cpcBidPrice    DECIMAL(19,10),
    cpmBidPrice    DECIMAL(19,10),
    conversion     BIGINT,         -- 转化数，目前不要含展示和点击产生的
    allConversion  BIGINT,         -- 转化数，含展示和点击产生的
    revenue        DECIMAL(19,10), -- 收益
    realRevenue    DECIMAL(19,10), -- 真实收益
    newCount       BIGINT,
    activeCount    BIGINT,
    l_time         VARCHAR(200),
    b_date         VARCHAR(200),

    publisherAmId     integer,
    publisherAmName   character varying,
    appModeId         integer,     -- 流量类型
    appModeName       character varying,
    publisherName     character varying,
    appName           character varying(100),
    countryName       character varying(100),
    carrierName       character varying,
    versionId         integer,
    versionName       character varying,
    publisherProxyId  integer,
    exchange_partition varchar(100)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(exchange_partition)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_report_publisher_third_income_dm_default_partition')
);


-- hive
drop view if exists ssp_report_publisher_dm;
create view ssp_report_publisher_dm as
select
    nvl(dwr.publisherId, a.publisherId) as publisherid ,-- INT, 流处理join的，可能app配置表还没有同步数据过来，因此这里最好再join一次
    dwr.appId          ,-- INT,
    dwr.countryId      ,-- INT,
    dwr.carrierId      ,-- INT,
--    dwr.versionName    ,-- STRING,         -- eg: v1.2.3
    dwr.affSub         ,-- STRING,
    dwr.requestCount   ,-- BIGINT,
    dwr.sendCount      ,-- BIGINT,
    dwr.showCount      ,-- BIGINT,
    dwr.clickCount     ,-- BIGINT,
    dwr.feeReportCount ,-- BIGINT,         -- 计费条数
    dwr.feeSendCount   ,-- BIGINT,         -- 计费显示条数
    dwr.feeReportPrice ,-- DECIMAL(19,10), -- 计费金额(真实收益)
    dwr.feeSendPrice   ,-- DECIMAL(19,10), -- 计费显示金额(收益)
    dwr.cpcBidPrice    ,-- DECIMAL(19,10),
    dwr.cpmBidPrice    ,-- DECIMAL(19,10),
    dwr.conversion     ,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    dwr.allConversion  ,-- BIGINT,         -- 转化数，含展示和点击产生的
    dwr.revenue        ,-- DECIMAL(19,10), -- 收益
    dwr.realRevenue    ,-- DECIMAL(19,10), -- 真实收益
    dwr.newCount       ,-- BIGINT,
    dwr.activeCount    ,-- BIGINT
    dwr.l_time,
    dwr.b_date,

    p.amId              as publisheramid,
    e.name              as publisheramname,
    a.mode              as appmodeid, --流量类型id
    m.name              as appmodename,
    p.name              as publishername,
    a.name              as appname,
    c.name              as countryname,
    ca.name             as carriername,
    v.id                as versionid,
    dwr.versionName     as versionname,
    e.proxyId           as publisherproxyid,
    c.alpha2_code       as countrycode,

    dwr.packageName,
    dwr.domain,
    dwr.operatingSystem,
    dwr.systemLanguage,
    dwr.deviceBrand,
    dwr.deviceType,
    dwr.browserKernel,
    dwr.b_time,
    co.id       as companyid,
    co.name     as companyname,
    nvl(p.ampaId, a_p.ampaId) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    nvl(p_amp.name, ap_amp.name) as publisherampaname
from ssp_report_publisher_dwr dwr
left join publisher p       on p.id = dwr.publisherId
LEFT JOIN employee e        on e.id = p.amId
left join app a             on a.id = dwr.appId
left join app_mode m        on m.id = a.mode
left join country c         on c.id = dwr.countryId
left join carrier ca        on ca.id = dwr.carrierId
left join version_control v on v.version = dwr.versionName

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee ap_amp   on ap_amp.id  = a_p.ampaId
where v.id is not null and b_date >= "2017-12-18"
union  all
select
    a.publisherId       as publisherid   ,
    t.appId             as appid    ,
    t.countryId         as countryid,
    -1                  as carrierid     ,
    'third-income'      as affsub,
    0                   as requestcount   ,
    0                   as sendcount      ,
    0                   as showcount      ,
    t.Pv                as clickcount  ,
    0                   as feereportcount ,
    0                   as feesendcount   ,
    0                   as feereportprice ,
    0                   as feesendprice   ,
    0                   as cpcbidprice    ,
    0                   as cpmbidprice    ,
    0                   as conversion     ,
    0                   as allconversion  ,
    t.thirdSendFee      as revenue,
    t.thirdFee          as realrevenue ,
    0                   as newcount       ,
    0                   as activecount    ,
    '2000-01-01 00:00:00' as l_time,
    statDate            as b_date  ,

    p.amId              as publisheramid,
    e.name              as publisheramname,
    a.mode              as appmodeid, --流量类型id
    m.name              as appmodename,
    p.name              as publishername,
    a.name              as appname,
    c.name              as countryname,
    'third-income'      as carriername,
    -1                  as versionid,
    'third-income'      as versionname,
    e.proxyId           as publisherproxyid,
    c.alpha2_code       as countrycode,

    'third-income' as packageName,
    'third-income' as domain,
    'third-income' as operatingSystem,
    'third-income' as systemLanguage,
    'third-income' as deviceBrand,
    'third-income' as deviceType,
    'third-income' as browserKernel,
    concat(statDate, ' 00:00:00') as b_time,
    co.id          as companyid,
    co.name        as companyname,
    nvl(p.ampaId, a_p.ampaId) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    nvl(p_amp.name, ap_amp.name) as publisherampaname
from ssp_report_publisher_third_income t
left join app a             on a.id = t.appId
left join publisher p       on p.id = a.publisherId
left join employee e        on e.id = p.amid
left join app_mode m        on m.id = a.mode
left join country c         on c.id = t.countryId

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee ap_amp   on ap_amp.id  = a_p.ampaId
--left join version_control v on v.version = t.sdkversion
where statDate >= "2017-12-14"  -- 原系统第三方数据导入截止到了2017-12-13
union all
select
    a.publisherId       as publisherid ,-- INT, 流处理join的，可能app配置表还没有同步数据过来，因此这里最好再join一次
    x.appId          ,-- INT,
    x.countryId      ,-- INT,
    -1 as carrierId      ,-- INT,
    null as affSub         ,-- STRING,
    x.requestCount   ,-- BIGINT,
    x.sendCount      ,-- BIGINT,
    x.showCount      ,-- BIGINT,
    x.thirdclickcount + x.clickCount     ,-- BIGINT,

    x.feecount          as feeReportCount ,-- BIGINT,         -- 计费条数
    x.sendfeecount      as feeSendCount   ,-- BIGINT,         -- 计费显示条数
    x.fee               as feeReportPrice ,-- DECIMAL(19,10), -- 计费金额(真实收益)
    x.sendfee           as feeSendPrice   ,-- DECIMAL(19,10), -- 计费显示金额(收益)
    0                   as cpcBidPrice    ,-- DECIMAL(19,10),
    0                   as cpmBidPrice    ,-- DECIMAL(19,10),
    x.feecount          as conversion     ,-- BIGINT,         -- 转化数，目前不要含展示和点击产生的
    0                   as allConversion  ,-- BIGINT,         -- 转化数，含展示和点击产生的
    nvl(x.thirdsendfee,0.0) + x.sendfee       as revenue        ,-- DECIMAL(19,10), -- 收益
    nvl(x.thirdfee,0.0) + x.fee               as realRevenue    ,-- DECIMAL(19,10), -- 真实收益
    x.usercount         as newCount       ,-- BIGINT,
    x.activeCount    ,-- BIGINT
    '2000-01-01 00:00:00' as l_time,
    x.statdate as b_date,

    p.amId              as publisheramid,
    e.name              as publisheramname,
    a.mode              as appmodeid, --流量类型id
    m.name              as appmodename,
    p.name              as publishername,
    a.name              as appname,
    c.name              as countryname,
    null                as carriername,
    v.id                as versionid,
    x.sdkversion        as versionname,
    e.proxyId           as publisherproxyid,
    c.alpha2_code       as countrycode,

    null        as packageName,
    null        as domain,
    null        as operatingSystem,
    null        as systemLanguage,
    null        as deviceBrand,
    null        as deviceType,
    null        as browserKernel,
    concat(statDate, ' 00:00:00') as b_time,
    co.id       as companyid,
    co.name     as companyname,
    nvl(p.ampaId, a_p.ampaId) as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    nvl(p_amp.name, ap_amp.name) as publisherampaname
from publisher_stat x
left join app a             on a.id = x.appId
left join publisher p       on p.id = a.publisherId
left join employee e        on e.id = p.amid
left join app_mode m        on m.id = a.mode
left join country c         on c.id = x.countryId
left join version_control v on v.version = x.sdkversion

left join publisher a_p     on  a_p.id = a.publisherId
left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = ap_am.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee ap_amp   on ap_amp.id  = a_p.ampaId
where v.id is not null and statdate < "2017-12-18";


create table app_mode(id INT, name STRING) STORED AS ORC;

INSERT OVERWRITE TABLE app_mode
VALUES
(1, 'SDK'),
(2, 'Smartlink'),
(3, 'Online API'),
(4, 'JS Tag'),
(5, 'RTB'),
(6, 'Offline API'),
(7, 'Video Sdk');





------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table ssp_report_publisher_dwr_view
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_report_publisher_dwr_view;
create view ssp_report_publisher_dwr_view as
select
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
    affSub,
    sum(requestCount)      as requestCount,
    sum(sendCount)         as sendCount,
    sum(showCount)         as showCount,
    sum(clickCount)        as clickCount,
    sum(feeReportCount)    as feeReportCount,
    sum(feeSendCount)      as feeSendCount,
    sum(feeReportPrice)    as feeReportPrice,
    sum(feeSendPrice)      as feeSendPrice,
    sum(cpcBidPrice)       as cpcBidPrice,
    sum(cpmBidPrice)       as cpmBidPrice,
    sum(conversion)        as conversion,
    sum(allConversion)     as allConversion,
    sum(revenue)           as revenue,
    sum(realRevenue)       as realRevenue,
    sum(newCount)          as newCount,
    sum(activeCount)       as activeCount,

    cast(null as string) packageName,
    cast(null as string) domain,
    cast(null as string) operatingSystem,
    cast(null as string) systemLanguage,
    cast(null as string) deviceBrand,
    cast(null as string) deviceType,
    cast(null as string) browserKernel,
    cast(null as string) b_time,

    l_time,
    b_date
from (
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        sum(times) as requestCount,
        0          as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        0          as newCount,
        0          as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fill_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        sum(times) as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        0          as newCount,
        0          as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_send_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        0          as sendCount,
        sum(times) as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        sum(cpmBidPrice)  as cpmBidPrice,
        0                 as conversion,
        sum(cpmTimes)     as allConversion,
        sum(cpmSendPrice) as revenue,
        sum(cpmBidPrice)  as realRevenue,
        0          as newCount,
        0          as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_show_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        0          as sendCount,
        0          as showCount,
        sum(times) as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        sum(cpcBidPrice)       as cpcBidPrice,
        0                      as cpmBidPrice,
        0                      as conversion,
        sum(cpcTimes)          as allConversion,
        sum(cpcSendPrice)      as revenue,
        sum(cpcBidPrice)       as realRevenue,
        0                      as newCount,
        0                      as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_click_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        0          as sendCount,
        0          as showCount,
        0          as clickCount,
        sum(times)       as feeReportCount,
        sum(sendTimes)   as feeSendCount,
        sum(reportPrice) as feeReportPrice,
        sum(sendPrice)   as feeSendPrice,
        0                as cpcBidPrice,
        0                as cpmBidPrice,
        sum(times)       as conversion,
        sum(times)       as allConversion,
        sum(sendPrice)   as revenue,
        sum(reportPrice) as realRevenue,
        0                as newCount,
        0                as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_fee_dwr
    where second(l_time) = 0
    group by
        publisherId,
        subId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        app.publisherId,
        dwr.appId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        0          as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        sum(newCount) as newCount,
        0             as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_user_new_dwr dwr
    left join app on app.id  = dwr.appId
    where second(l_time) = 0
    group by
        app.publisherId,
        dwr.appId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date

    UNION ALL
    select
        app.publisherId,
        dwr.appId,
        countryId,
        carrierId,
        sv,
        affSub,
        0          as requestCount,
        0          as sendCount,
        0          as showCount,
        0          as clickCount,
        0          as feeReportCount,
        0          as feeSendCount,
        0          as feeReportPrice,
        0          as feeSendPrice,
        0          as cpcBidPrice,
        0          as cpmBidPrice,
        0          as conversion,
        0          as allConversion,
        0          as revenue,
        0          as realRevenue,
        0          as newCount,
        sum(activeCount) as activeCount,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from ssp_user_active_dwr dwr
    left join app on app.id  = dwr.appId
    where second(l_time) = 0
    group by
        app.publisherId,
        dwr.appId,
        countryId,
        carrierId,
        sv,
        affSub,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
        b_date
)t0
group by
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
    affSub,
    l_time,
    b_date;



--CREATE TABLE IF NOT EXISTS ssp_report_app_dwr_tmp like ssp_report_app_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_report_publisher_dwr partition(l_time, b_date)
select * from ssp_report_publisher_dwr_view
where l_time >='2017-10-18 00:00:00' and l_time <'2017-10-23 00:00:00';











set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_report_publisher_dwr partition(l_time, b_date)
select * from ssp_report_publisher_dwr_view
where b_date >='2017-10-18' and b_date <='2017-10-22';




select sum(clickCount), b_date from ssp_report_publisher_dwr_view
where b_date >='2017-10-18' and b_date <='2017-10-23'
group by b_date;



select sum(clickCount), b_date from ssp_report_publisher_dwr
where b_date >='2017-10-18' and b_date <='2017-10-23'
group by b_date;



select sum(clickCount), b_date from ssp_report_campaign_dwr
where b_date >='2017-10-18' and b_date <='2017-10-23'
group by b_date;



select count(1), b_date from ssp_click_dwi
where  b_date <='2017-10-23'
group by b_date



--





-- FOR Handler
--create table ssp_offer_dwr(
--    offerId         INT,
--    todayClickCount BIGINT,
--    todayShowCount  BIGINT,
--    todayFeeCount       BIGINT,        --当天计费条数
--    todayFee        DECIMAL(19,10), --当天计费条数
--)
--PARTITIONED BY (l_time STRING, b_date STRING)
--STORED AS ORC;


insert overwrite table ssp_report_publisher_dwr partition(l_time, b_date)
select
    publisherId,
    appId,
    countryId,
    carrierId,
    versionName,
    affSub,
    requestCount      as requestCount,
    sendCount       as sendCount,
    showCount         as showCount,
    clickCount        as clickCount,
    feeReportCount    as feeReportCount,
    feeSendCount     as feeSendCount,
    feeReportPrice    as feeReportPrice,
    feeSendPrice     as feeSendPrice,
    cpcBidPrice       as cpcBidPrice,
    cpmBidPrice       as cpmBidPrice,
    conversion        as conversion,
    allConversion     as allConversion,
    revenue           as revenue,
    realRevenue       as realRevenue,
    newCount          as newCount,
    activeCount       as activeCount,

    null      as packageName,
    null      as  domain,
    null       as  operatingSystem,
    null        as systemLanguage,
    null           as deviceBrand,
    null            as deviceType,
    null         as browserKernel,
    null              as  b_time,

    l_time,
    b_date
from ssp_report_publisher_dwr
where b_date in ( "2017-12-23" , "2017-12-22")


--
--
--SET spark.default.parallelism = 3;
--SET    spark.sql.shuffle.partitions = 3;
--
--
--select
--  b_date,
--  0                             as ssp_report_overall_dwr_day,
--  SUM(realrevenue)                   as ssp_report_overall_dwr
--from ssp_report_overall_dwr_day
--where b_date in ("2018-04-22")
--group by b_date;
--
--
--
--select
--  b_date,
--  SUM(realrevenue)                   as ssp_report_overall_dwr_day
--from ssp_report_overall_dwr_day
--where  b_date in ("2018-04-23")
--group by b_date;
