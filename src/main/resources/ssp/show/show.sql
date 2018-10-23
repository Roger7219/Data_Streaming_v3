-------------------------------------------------------------
-- 展示数据
-------------------------------------------------------------
CREATE TABLE ssp_show_dwi(
    repeats     INT   ,
    rowkey      STRING,
    id          INT   ,
    publisherId INT   , -- + AM
    subId       INT   , -- + APP
    offerId     INT   , -- + offer.adtype(eg:,4,5, 数据接口定义的名称是adFormatId) ,offer.optstatus
    campaignId  INT   , -- + campaign > 上级ADVER, > campaign =>adCategory1
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
    s5          STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;


ALTER TABLE ssp_show_dwi ADD COLUMNS (affSub STRING)
//dwi 追加s3-s5三個字段
ALTER TABLE ssp_show_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE ssp_show_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE ssp_show_dwi ADD COLUMNS (s5 STRING);

--add new clomns 12/21
ALTER TABLE ssp_show_dwi ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_show_dwi ADD COLUMNS (domain STRING);

-- 展示数据统计: 时间，广告主，Campaign，Offer，AM，publisher，app，国家，运营商，版本号，请求广告类型，统计结果包括展示数据
-- 广告计费类型是cpc时需要统计成计费数据
create table ssp_show_dwr(
    publisherId  INT,
    subId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    adType       INT,
    campaignId   INT,
    offerId      INT,
    imageId      INT,
    affSub       STRING,
    times        BIGINT,
    cpmTimes     BIGINT,
    cpmBidPrice  DECIMAL(19,10),
    cpmSendPrice DECIMAL(19,10)
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/21
ALTER TABLE ssp_show_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (domain STRING);

ALTER TABLE ssp_show_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_show_dwr ADD COLUMNS (b_time STRING);


-- Order: dwrGroupBy b_date aggExprsAlias l_time
CREATE TABLE SSP_SHOW_DM_PHOENIX (
    PUBLISHERID  INTEGER NOT NULL,
    SUBID        INTEGER NOT NULL,
    COUNTRYID    INTEGER NOT NULL,
    CARRIERID    INTEGER NOT NULL,
    SV           VARCHAR NOT NULL,
    ADTYPE       INTEGER NOT NULL,
    CAMPAIGNID   INTEGER NOT NULL,
    OFFERID      INTEGER NOT NULL,
    B_DATE       VARCHAR NOT NULL,
    TIMES        BIGINT,
    L_TIME       VARCHAR,
    --    upsert into 字符串字段值要用单引号！！！
    constraint pk primary key (PUBLISHERID,SUBID,COUNTRYID,CARRIERID,SV,ADTYPE,CAMPAIGNID,OFFERID,B_DATE)
);


------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_show_dwr_view;
create view ssp_show_dwr_view as
select
    publisherId as publisherId,
    subId       as subId,
    countryId   as countryId,
    carrierId   as carrierId,
    sv          as sv,
    adType      as adType,
    campaignId  as campaignId,
    offerId     as offerId,
    imageId     as imageId,
    affSub      as affSub,
    count(1)    as times,
    count( if( priceMethod = 2, 1, null) ) as cpmTimes,
    sum( cast( if( priceMethod = 2,  bidPrice, 0) as decimal(19,10) ) ) / 1000 as cpmBidPrice,
    sum( cast( if( priceMethod = 2,  sendPrice, 0) as decimal(19,10) ) ) as cpmSendPrice,
     date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
     b_date

from ssp_show_dwi
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



--create table ssp_show_dwr_tmp like ssp_show_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ssp_show_dwr partition(l_time, b_date)
select * from ssp_show_dwr_view
where where second(l_time) = 0;
--l_time <='2017-09-09 00:00:00';









insert overwrite table ssp_show_dwr partition(l_time, b_date)
select
publisherId  ,
subId        ,
countryId    ,
carrierId    ,
sv           ,
adType       ,
campaignId   ,
offerId      ,
count(1) as times        ,
count( if( priceMethod = 2, 1, null) ) as cpmTimes     ,
sum( cast( if( priceMethod = 2,  bidPrice, 0) as decimal(19,10) ) ) / 1000  as cpmBidPrice  ,
sum( cast( if( priceMethod = 2,  sendPrice, 0) as decimal(19,10) ) ) / 1000 as cpmSendPrice,
'2017-07-16 00:00:00' as l_time,
split(createTime, ' ')[0]  as b_date
from ssp_show_dwi
group by
publisherId  ,
subId        ,
countryId    ,
carrierId    ,
sv           ,
adType       ,
campaignId   ,
offerId,
split(createTime, ' ')[0]



