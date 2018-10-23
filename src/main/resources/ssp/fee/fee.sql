-------------------------------------------------------------
-- 转化（计费）数据
-------------------------------------------------------------
CREATE TABLE ssp_fee_dwi(
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
    priceMethod INT   , -- 1 cpc(click), 2 cpm(show), 3 cpa
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

ALTER TABLE ssp_fee_dwi ADD COLUMNS (affSub STRING);
--add s3-s5
ALTER TABLE ssp_fee_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE ssp_fee_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE ssp_fee_dwi ADD COLUMNS (s5 STRING);

--add new clomns 12/21
ALTER TABLE ssp_fee_dwi ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_fee_dwi ADD COLUMNS (domain STRING);


create table ssp_fee_dwr(
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
    times        BIGINT,        --计费条数
    sendTimes    BIGINT,--计费显示条数
    reportPrice  DECIMAL(19,10),--计费金额(真实收益)
    sendPrice    DECIMAL(19,10) --计费显示金额(收益)
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add cloumn
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpcTimes BIGINT);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpcReportPrice decimal(19,10));
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpcSendPrice decimal(19,10));
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpmTimes BIGINT);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpmReportPrice decimal(19,10));
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpmSendPrice decimal(19,10));
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpaTimes BIGINT);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpaSendTimes BIGINT);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpaReportPrice decimal(19,10));
ALTER TABLE ssp_fee_dwr ADD COLUMNS (feeCpaSendPrice decimal(19,10));

--add new clomns 2017/12/21
ALTER TABLE ssp_fee_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (domain STRING);

ALTER TABLE ssp_fee_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_fee_dwr ADD COLUMNS (b_time STRING);

-- addd new clomns 2018/02/07
ALTER TABLE ssp_fee_dwi ADD COLUMNS (reportIp String);


-- Order: dwrGroupBy b_date aggExprsAlias l_time
CREATE TABLE SSP_FEE_DM_PHOENIX (
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
drop view if exists ssp_fee_dwr_view;
create view ssp_fee_dwr_view as
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
    count(1)   as times,
    count(if(isSend = 1, 1, null)) as sendTimes,
    sum( cast(reportPrice as decimal(19,10)) ) as reportPrice,
    sum( cast(sendPrice as decimal(19,10)) )  as sendPrice,
    count(if( priceMethod = 1,  1, null)) as feeCpcTimes,
    sum( cast( if( priceMethod = 1,  reportPrice, 0) as decimal(19,10) ) ) as feeCpcReportPrice,
    sum( cast( if( priceMethod = 1,  sendPrice, 0) as decimal(19,10) ) ) as feeCpcSendPrice,
    count(if( priceMethod = 2,  1, null)) as feeCpmTimes,
    sum( cast( if( priceMethod = 2,  reportPrice, 0) as decimal(19,10) ) ) as feeCpmReportPrice,
    sum( cast( if( priceMethod = 2,  sendPrice, 0) as decimal(19,10) ) ) as feeCpmSendPrice,
    count(if( priceMethod = 3,  1, null)) as feeCpaTimes,
    count(if( priceMethod = 3 and isSend = 1,  1, null)) as feeCpaSendTimes,
    sum( cast( if( priceMethod = 3,  reportPrice, 0) as decimal(19,10) ) ) as feeCpaReportPrice,
    sum( cast( if( priceMethod = 3 and isSend = 1,  sendPrice, 0) as decimal(19,10) ) ) as feeCpaSendPrice,
    cast(null as string) as packageName,
    cast(null as string) as domain,
    cast(null as string) as operatingSystem,
    cast(null as string) as systemLanguage,
    cast(null as string) as deviceBrand,
    cast(null as string) as deviceType,
    cast(null as string) as browserKernel,
    cast(null as string) as b_time,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
    b_date
from ssp_fee_dwi
where repeated ='N'
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
  b_date
  ;

set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

--create table ssp_fee_dwr_tmp like ssp_fee_dwr;
insert overwrite table ssp_fee_dwr partition(l_time, b_date)
select * from ssp_fee_dwr_view
where second(l_time) = 0;


--where l_time <='2017-09-09 00:00:00';