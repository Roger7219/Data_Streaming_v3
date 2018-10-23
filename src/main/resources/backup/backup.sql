-------------------------------------------------------------
-- 请求数据
-------------------------------------------------------------
CREATE TABLE backup__ssp_fee_dwi(
    repeats     INT   ,
    rowkey      STRING,
    id          INT   ,
    publisherId INT   , -- + AM
    subId       INT   , -- + APP
    offerId     INT   ,
    campaignId  INT   , -- + campaign > 上级ADVER
    countryId   INT   ,
    carrierId   INT   ,
    deviceType  INT   ,
    userAgent   STRING,
    ipAddr      STRING,
    clickId     STRING,
    price       DOUBLE,
    reportTime  STRING,
    createTime  STRING,
    clickTime   STRING,
    showTime    STRING,
    requestType STRING,
    priceMethod INT   ,
    bidPrice    DOUBLE,
    adType      INT   ,
    isSend      INT   ,
    reportPrice DOUBLE,
    sendPrice   DOUBLE,
    s1          STRING,
    s2          STRING,
    gaid        STRING,
    androidId   STRING,
    idfa        STRING,
    postBack    STRING,
    sendStatus  INT   ,
    sendTime    STRING,
    sv          STRING,
    imei        STRING,
    imsi        STRING,
    imageId     INT,
    affSub      STRING,
    s3          STRING,
    s4          STRING,
    s5          STRING,
    packageName STRING, -- 包名
    domain      STRING,  -- 域名
    respStatus  INT,
    winPrice    DOUBLE,
    winTime     String,
    appPrice    DOUBLE,
    test        INT
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;


create table fill_test2(s1 STRING)
STORED AS ORC;


ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (affSub STRING);
//dwi 追加s3-s5三個字段
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (s5 STRING);


ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (ruleId int);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (smartId int);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (reportIp String);


ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (eventName String);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (eventValue int);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (refer String);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (status int);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (city String);
ALTER TABLE backup__ssp_fee_dwi ADD COLUMNS (region String);

ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (eventName String);
ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (eventValue int);
ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (refer String);
ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (status int);
ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (city String);
ALTER TABLE backup__ssp_fee_dwi2 ADD COLUMNS (region String);

ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (respStatus INT);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (winPrice double);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (winTime string);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (appPrice double);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (test INT);
ALTER TABLE backup__ssp_fill_dwi ADD COLUMNS (smartId INT);


ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (browserKernel2 STRING) FIRST;

ALTER TABLE contacts ADD email VARCHAR(60) ;

-- 请求数据不含campaignid, offerid
-- 请求数据统计：时间，AM，publisher，app，国家，运营商，版本号，请求广告类型，agg:请求量

ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (packageName STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (domain STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (operatingSystem STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (systemLanguage STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (deviceBrand STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (deviceType STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (browserKernel STRING) ;
ALTER TABLE backup__ssp_fill_dwr ADD COLUMNS (b_time STRING) ;



create table backup__ssp_fill_dwr(
    publisherId  INT,
    subId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    adType       INT,
    campaignId   INT,
    offerId      Int,
    imageId      INT,
    affSub       STRING,
    times           BIGINT
    ,
    packageName     STRING,
    domain          STRING,
    operatingSystem STRING,
    systemLanguage  STRING,
    deviceBrand     STRING,
    deviceType      STRING,
    browserKernel   STRING
)
PARTITIONED BY (l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;


create table backup__ssp_fee_dwr(
    publisherId int,
    times       bigint,
    times2      bigint
)
PARTITIONED BY (l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;


--
--create table backup2(
--    times           BIGINT
--)
--PARTITIONED BY (l_time STRING)
--STORED AS ORC;
--
--insert into table backup2 values(1, "2015-02-26");
--
--create table backup3 like backup2 ;
--
--insert overwrite table backup2 select * from backup3;
--
--alter table backup3 add partition (l_time='2015-02-26')

CREATE TABLE  SSP_FILL_DM_PHOENIX (
    PUBLISHERID  INTEGER NOT NULL,
    SUBID        INTEGER NOT NULL,
    COUNTRYID    INTEGER NOT NULL,
    CARRIERID    INTEGER NOT NULL,
    SV           VARCHAR NOT NULL,
    ADTYPE       INTEGER NOT NULL,
    B_DATE       VARCHAR NOT NULL,
    TIMES        BIGINT,
    L_TIME       VARCHAR,
    --    upsert into 字符串字段值要用单引号！！！
    constraint pk primary key (PUBLISHERID,SUBID,COUNTRYID,CARRIERID,SV,ADTYPE,B_DATE)
);


------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists backup__ssp_fill_dwr_view;
create view backup__ssp_fill_dwr_view as
select
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
    adType,
    campaignId,
    offerId,
    imageId,
    affSub,
    count(1) as times,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
     b_date
from backup__ssp_fill_dwi
group by
    publisherId,
    subId,
    countryId,
    carrierId,
    sv,
    adType,
    campaignId,
    offerId,
    imageId,
    affSub,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
    b_date;


--create table if not exists backup__ssp_fill_dwr_tmp like backup__ssp_fill_dwr;

set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table backup__ssp_fill_dwr partition(l_time, b_date)
select * from backup__ssp_fill_dwr_view
where second(l_time) = 0;
--where l_time <='2017-09-09 00:00:00';








--------------------- BATCH TEST ---------------------

SELECT
b_date, count(1),
FROM backup__ssp_fill_dwi
GROUP BY b_date;

SELECT
b_date, sum(times)
from backup__ssp_fill_dwr
group by b_date;

--

SELECT
b_date, count(1)
FROM ssp_send_dwi
GROUP BY b_date;

SELECT
b_date, sum(times)
FROM ssp_send_dwr
GROUP BY b_date;

--

SELECT
b_date, count(1)
FROM ssp_show_dwi;