-------------------------------------------------------------
-- 重复订阅数据
-------------------------------------------------------------
CREATE TABLE ssp_dupscribe_dwi(
    repeats     INT   ,
    rowkey      STRING,
    id          INT   ,
    publisherId INT   , -- + AM
    subId       INT   , -- + APP
    offerId     INT   ,
    campaignId  INT   , -- + campaign > 上级ADVERTISER
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

ALTER TABLE ssp_dupscribe_dwi ADD COLUMNS (affSub STRING);
--add s3-s5
ALTER TABLE ssp_dupscribe_dwi ADD COLUMNS (s3 STRING);
ALTER TABLE ssp_dupscribe_dwi ADD COLUMNS (s4 STRING);
ALTER TABLE ssp_dupscribe_dwi ADD COLUMNS (s5 STRING);

create table ssp_dupscribe_dwr(
    offerId     INT,
    userCount   BIGINT,  -- 用户数
    scribeCount BIGINT   -- 订阅次数
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


create 'ssp_dupscribe_dwi_uuid', 'f'


------------------------------------------------------------------------------------
-- Hive FOR Greenplum
------------------------------------------------------------------------------------
-- 展示统计结果
drop view if exists ssp_dupscribe_dm;
create view ssp_dupscribe_dm as
select
    dwr.*,
    o.name      as offername
from ssp_dupscribe_dwr dwr
left join offer o on o.id = dwr.offerId;

-- 展示明细
drop view if exists ssp_dupscribe_detail_dm;
create view ssp_dupscribe_detail_dm as
select
    dwi.*,
    o.name          as offername,
    p.name          as publishername,
    c.name          as countryname,
    ca.name         as carriername,
    cam.adverId     as adverid,
    adv.name        as advername
from ssp_dupscribe_dwi dwi
left join offer o on o.id = dwi.offerId
left join publisher p on p.id = dwi.publisherId
left join country c on c.id = dwi.countryId
left join carrier ca on ca.id = dwi.carrierId
left join campaign cam on cam.id = dwi.campaignId
left join advertiser adv on adv.id = cam.adverId
where dwi.repeated = 'Y';


------------------------------------------------------------------------------------
-- FOR Greenplum
------------------------------------------------------------------------------------
drop table if exists ssp_dupscribe_dm ;
create table ssp_dupscribe_dm(
     offerid       int,
     usercount     bigint,
     scribecount   bigint,
     l_time        VARCHAR(100),
     b_date        VARCHAR(100),
     offername     VARCHAR(200)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_dupscribe_dm_default_partition')
);


drop table if exists ssp_dupscribe_detail_dm ;
create table ssp_dupscribe_detail_dm(
     repeats         int,
     rowkey          VARCHAR(500),
     id              int,
     publisherid     int,
     subid           int,
     offerid         int,
     campaignid      int,
     countryid       int,
     carrierid       int,
     devicetype      int,
     useragent       VARCHAR(500),
     ipaddr          VARCHAR(500),
     clickid         VARCHAR(500),
     price           DECIMAL(19,10),
     reporttime      VARCHAR(500),
     createtime      VARCHAR(500),
     clicktime       VARCHAR(500),
     showtime        VARCHAR(500),
     requesttype     VARCHAR(500),
     pricemethod     int,
     bidprice        DECIMAL(19,10),
     adtype          int,
     issend          int,
     reportprice     DECIMAL(19,10),
     sendprice       DECIMAL(19,10),
     s1              VARCHAR(500),
     s2              VARCHAR(500),
     gaid            VARCHAR(100),
     androidid       VARCHAR(500),
     idfa            VARCHAR(500),
     postback        VARCHAR(500),
     sendstatus      int,
     sendtime        VARCHAR(500),
     sv              VARCHAR(500),
     imei            VARCHAR(500),
     imsi            VARCHAR(500),
     imageid         int,
     affsub          VARCHAR(500),
     s3              VARCHAR(500),
     s4              VARCHAR(500),
     s5              VARCHAR(500),
     repeated        VARCHAR(500),
     l_time          VARCHAR(500),
     b_date          VARCHAR(500),
     offername       VARCHAR(500),
     publishername   VARCHAR(500),
     countryname     VARCHAR(500),
     carriername     VARCHAR(500),
     adverid         int,
     advername       VARCHAR(500)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_dupscribe_detail_dm_default_partition')
);


------------------------------------------------------------------------------------
-- FOR kylin
------------------------------------------------------------------------------------
-- 展示统计结果
drop view if exists ssp_dupscribe_dm;
create view ssp_dupscribe_dm as
select
    dwr.*,
    concat_ws('^', o.name , cast(dwr.offerId as string) )  as offerName
from ssp_dupscribe_dwr dwr
left join offer o on o.id = dwr.offerId;

-- 展示明细
drop view if exists ssp_dupscribe_detail_dm;
create view ssp_dupscribe_detail_dm as
select
    dwi.*,
    concat_ws('^', o.name , cast(dwi.offerId as string) )  as offerName,
    concat_ws('^', p.name , cast(dwi.publisherId as string) )  as publisherName,
    concat_ws('^', c.name , cast(dwi.countryId as string) )  as countryName,
    concat_ws('^', ca.name , cast(dwi.carrierId as string) )  as carrierName,
    cam.adverId as adverId,
    concat_ws('^', adv.name , cast(cam.adverId as string) )  as adverName
from ssp_dupscribe_dwi dwi
left join offer o on o.id = dwi.offerId
left join publisher p on p.id = dwi.publisherId
left join country c on c.id = dwi.countryId
left join carrier ca on ca.id = dwi.carrierId
left join campaign cam on cam.id = dwi.campaignId
left join advertiser adv on adv.id = cam.adverId
where dwi.repeated = 'Y';



