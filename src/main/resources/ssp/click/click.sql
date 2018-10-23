-------------------------------------------------------------
-- 点击数据
-------------------------------------------------------------
CREATE TABLE ssp_click_dwi(
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
    priceMethod INT   ,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
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
    affSub      INT,
    s3          STRING,
    s4          STRING,
    s5          STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

-- 添加列
ALTER TABLE ssp_click_dwi ADD COLUMNS (imageId STRING);
ALTER TABLE ssp_click_dwi CHANGE COLUMN imageId imageId INT ; --AFTER imsi;
ALTER TABLE ssp_click_dwi ADD COLUMNS (affSub STRING);
--add s3-s5
ALTER TABLE ssp_click_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE ssp_click_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE ssp_click_dwi ADD COLUMNS (s5 STRING);

--add new clomns 12/21
ALTER TABLE ssp_click_dwi ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_click_dwi ADD COLUMNS (domain STRING);

ALTER TABLE ssp_click_dwi ADD COLUMNS (uid STRING);
ALTER TABLE ssp_click_dwi ADD COLUMNS (times INT);
ALTER TABLE ssp_click_dwi ADD COLUMNS (time INT);
ALTER TABLE ssp_click_dwi ADD COLUMNS (isNew INT);

-- 点击数据统计: 时间，广告主，Campaign，Offer，AM，publisher，app，国家，运营商，版本号，请求广告类型，统计结果包括点击数据
create table ssp_click_dwr(
    publisherId  INT,
    subId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    adType       INT,
    campaignId INT,
    offerId      Int,
    imageId      INT,      -- +
    affSub       STRING,   -- +
    times        BIGINT,
    cpcTimes     BIGINT,
    cpcBidPrice  DECIMAL(19,10),
    cpcSendPrice DECIMAL(19,10)
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/21
ALTER TABLE ssp_click_dwr ADD COLUMNS (packageName STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (domain STRING);

ALTER TABLE ssp_click_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_click_dwr ADD COLUMNS (b_time STRING);


CREATE TABLE SSP_CLICK_DWI_PHOENIX(
    "repeats"     INTEGER   ,
    "rowkey"      VARCHAR,

    "id"          INTEGER,
    "publisherId" INTEGER, -- + AM
    "subId"       INTEGER, -- + APP
    "offerId"     INTEGER,
    "campaignId"  INTEGER, -- + campaign > 上级ADVER
    "countryId"   INTEGER,
    "carrierId"   INTEGER,
    "deviceType"  INTEGER,
    "userAgent"   VARCHAR,
    "ipAddr"      VARCHAR,
    "clickId"     VARCHAR,
    "price"       VARCHAR, --DOUBLE
    "reportTime"  VARCHAR,
    "createTime"  VARCHAR,
    "clickTime"   VARCHAR,
    "showTime"    VARCHAR,
    "requestType" VARCHAR,
    "priceMethod" INTEGER,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
    "bidPrice"    VARCHAR, -- DOUBLE
    "adType"      INTEGER,
    "isSend"      INTEGER,
    "reportPrice" VARCHAR, -- DOUBLE
    "sendPrice"   VARCHAR, -- DOUBLE
    "s1"          VARCHAR,
    "s2"          VARCHAR,
    "gaid"        VARCHAR,
    "androidId"   VARCHAR,
    "idfa"        VARCHAR,
    "postBack"    VARCHAR,
    "sendStatus"  INTEGER,
    "sendTime"    VARCHAR,
    "sv"          VARCHAR,
    "imei"        VARCHAR,
    "imsi"        VARCHAR,
    "imageId"     INTEGER,
    "affSub"      VARCHAR,

    "repeated"    VARCHAR,
    "l_time"      VARCHAR,
    "b_date"      VARCHAR
    constraint pk primary key ("clickId")
);



------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_click_dwr_view;
create view ssp_click_dwr_view as
select
    publisherId,
    subId      ,
    countryId  ,
    carrierId  ,
    sv         ,
    adType     ,
    campaignId ,
    offerId    ,
    imageId    ,
    affSub     ,
    count(1) as times,
    count(if( priceMethod = 1,  1, null)) as cpcTimes,
    sum( cast( if( priceMethod = 1,  bidPrice, 0) as decimal(19,10) ) ) as cpcBidPrice,
    sum( cast( if( priceMethod = 1,  sendPrice, 0) as decimal(19,10) ) ) as cpcSendPrice,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
    b_date
from ssp_click_dwi
group by
    publisherId,
    subId      ,
    countryId  ,
    carrierId  ,
    sv         ,
    adType     ,
    campaignId ,
    offerId    ,
    imageId    ,
    affSub     ,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
    b_date;


--create table ssp_click_dwr_tmp like ssp_click_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ssp_click_dwr partition(l_time, b_date)
select * from ssp_click_dwr_view
where second(l_time) = 0;

--and l_time >= '2017-09-01 00:00:00';


--where l_time <= from_unixtime(unix_timestamp(),  'yyyy-MM-dd HH:mm:ss')























===VALUE

set  spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table ssp_click_dwr_tmp partition(l_time, b_date)
select * from ssp_click_dwr_view
where b_date <'2017-09-10';

insert overwrite table ssp_fee_dwr_tmp partition(l_time, b_date)
select * from ssp_fee_dwr_view
where b_date <'2017-09-10';


insert overwrite table ssp_fill_dwr_tmp partition(l_time, b_date)
select * from ssp_fill_dwr_view
where b_date <'2017-09-10';

insert overwrite table ssp_send_dwr_tmp partition(l_time, b_date)
select * from ssp_send_dwr_view
where b_date <'2017-09-10';


insert overwrite table ssp_show_dwr_tmp partition(l_time, b_date)
select * from ssp_show_dwr_view
where b_date <'2017-09-10';


insert overwrite table ssp_user_keep_dwr_tmp partition(l_time, b_date)
select * from ssp_user_keep_dwr_view
where b_date <'2017-09-10';


insert overwrite table ssp_user_active_dwr_tmp partition (l_time, b_date)
select * from ssp_user_active_dwr_view
where b_date <'2017-09-10';

insert overwrite table ssp_user_new_dwr_tmp
select * from ssp_user_new_dwr_view
where b_date <'2017-09-10';

