-------------------------------------------------------------
-- 请求数据
-------------------------------------------------------------
CREATE TABLE ssp_fill_dwi(
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
    s5          STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

create table fill_test2(s1 STRING)
STORED AS ORC;


ALTER TABLE ssp_fill_dwi ADD COLUMNS (affSub STRING);
-- dwi 追加s3-s5三個字段
ALTER TABLE ssp_fill_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (s5 STRING);
-- 2017-12-22 dwi 新增字段
ALTER TABLE ssp_fill_dwi ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (domain STRING);

--18/06/13
ALTER TABLE ssp_fill_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_fill_dwi ADD COLUMNS (isNew INT);


-- 2017-12-22 dwr 新增字段
ALTER TABLE ssp_fill_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (domain STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_fill_dwr ADD COLUMNS (b_time STRING);


-- 请求数据不含campaignid, offerid
-- 请求数据统计：时间，AM，publisher，app，国家，运营商，版本号，请求广告类型，agg:请求量
create table ssp_fill_dwr(
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
    times        BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


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
drop view if exists ssp_fill_dwr_view;
create view ssp_fill_dwr_view as
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
    cast(null as STRING) as packageName,
    cast(null as STRING) as domain,
    cast(null as STRING) as operatingSystem,
    cast(null as STRING) as systemLanguage,
    cast(null as STRING) as deviceBrand,
    cast(null as STRING) as deviceType,
    cast(null as STRING) as browserKernel,
    cast(null as STRING) as b_time,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
    b_date
from ssp_fill_dwi
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


--create table if not exists ssp_fill_dwr_tmp like ssp_fill_dwr;

set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ssp_fill_dwr partition(l_time, b_date)
select * from ssp_fill_dwr_view
where second(l_time) = 0;
--where l_time <='2017-09-09 00:00:00';








--------------------- BATCH TEST ---------------------

SELECT
b_date, count(1),
FROM ssp_fill_dwi
GROUP BY b_date;

SELECT
b_date, sum(times)
from ssp_fill_dwr
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

set l_time = "2012-12-12 00:00:00";

drop view if exists camapgin_dm ;

truncate table camapgin_dm__base;

insert overwrite table camapgin_dm__base
select * from camapgin_dm__base
union all
select
  campaignId,
  CAST( 100000*sum(cpmBidPrice) AS INT64)/100000.0  as cpmBidPrice
from report_dataset.ssp_report_campaign_dm
where cpmBidPrice > 0 and (data_type = 'camapgin' or data_type is null) and {l_time}--l_time < ${l_time}
group by campaignId;

set l_time = "2012-12-13 00:00:00"
drop view if exists camapgin_dm__incr;
create view camapgin_dm__incr
select
  campaignId,
  CAST( 100000*sum(cpmBidPrice) AS INT64)/100000.0  as cpmBidPrice
from report_dataset.ssp_report_campaign_dm
where cpmBidPrice > 0 and (data_type = 'camapgin' or data_type is null) and l_time >= ${l_time}
group by campaignId;

drop view if exists camapgin_dm;
create view camapgin_dm as
select * from camapgin_dm__base
union all
select * from camapgin_dm__incr