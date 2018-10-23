-------------------------------------------------------------
-- dsp kafka原始数据
-------------------------------------------------------------
CREATE TABLE dsp_traffic_dwi(
    repeats      INT,
    rowkey       STRING,
    `timestamp`  BIGINT,
    event_type   STRING,
    event_key    STRING,
    request      STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : BIGINT,
                         app_id        : BIGINT,
                         country_id    : BIGINT,
                         carrier_id    : BIGINT,
                         ip           :  STRING,
                         raw_request   : STRING
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,
                         dsp_id        : BIGINT,
                         bid_price     : DOUBLE,
                         clear_price   : DOUBLE,
                         media_price   : DOUBLE,
                         win_noticeUrl : STRING,
                         click_url     : STRING,
                         pixel_url     : STRING,
                         raw_response  : STRING
                     >
                 >,
    send         STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : BIGINT,
                         app_id        : BIGINT,
                         country_id    : BIGINT,
                         carrier_id    : BIGINT,
                         ip            : STRING,
                         raw_request   : STRING
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,
                         dsp_id        : BIGINT,
                         bid_price     : DOUBLE,
                         clear_price   : DOUBLE,
                         media_price   : DOUBLE,
                         win_noticeUrl : STRING,
                         click_url     : STRING,
                         pixel_url     : STRING,
                         raw_response  : STRING
                     >
                 >,
     win_notice  STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : BIGINT,
                         app_id        : BIGINT,
                         country_id    : BIGINT,
                         carrier_id    : BIGINT,
                         ip            : STRING,
                         raw_request   : STRING
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,
                         dsp_id        : BIGINT,
                         bid_price     : DOUBLE,
                         clear_price   : DOUBLE,
                         media_price   : DOUBLE,
                         win_noticeUrl : STRING,
                         click_url     : STRING,
                         pixel_url     : STRING,
                         raw_response  : STRING
                     >
                 >,
    impression   STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : BIGINT,
                         app_id        : BIGINT,
                         country_id    : BIGINT,
                         carrier_id    : BIGINT,
                         ip            : STRING,
                         raw_request   : STRING
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,
                         dsp_id        : BIGINT,
                         bid_price     : DOUBLE,
                         clear_price   : DOUBLE,
                         media_price   : DOUBLE,
                         win_noticeUrl : STRING,
                         click_url     : STRING,
                         pixel_url     : STRING,
                         raw_response  : STRING
                     >
                 >,
     click       STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : BIGINT,
                         app_id        : BIGINT,
                         country_id    : BIGINT,
                         carrier_id    : BIGINT,
                         ip            : STRING,
                         raw_request   : STRING
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,
                         dsp_id        : BIGINT,
                         bid_price     : DOUBLE,
                         clear_price   : DOUBLE,
                         media_price   : DOUBLE,
                         win_noticeUrl : STRING,
                         click_url     : STRING,
                         pixel_url     : STRING,
                         raw_response  : STRING
                     >
                 >
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;


create table dsp_traffic_dwr(
    publisherId    BIGINT,
    appId          BIGINT,
    dspId          BIGINT,
    countryId      BIGINT,
    carrierId      BIGINT,
    utcDay         STRING,
    cstDay         STRING,
    requests       BIGINT,
    sends          BIGINT,
    winNotices     BIGINT,
    impressions    BIGINT,
    clicks         BIGINT,
    mediaCost      DECIMAL(19,10), -- fee
    sspRevenue     DECIMAL(19,10),
    clearPrice     DECIMAL(19,10)  -- sendFee
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


-- Hive For Greenplum
drop view if exists dsp_traffic_dm;
create view dsp_traffic_dm as
select
    dwr.publisherId as publisherid,
    dwr.appId       as appid,
    dwr.dspId       as dspid,
    dwr.countryId   as countryid,
    dwr.carrierId   as carrierid,
    dwr.utcDay      as utcday,
    dwr.cstDay      as cstday,
    dwr.requests    as requests,
    dwr.sends       as sends,
    dwr.winNotices  as winnotices,
    dwr.impressions as impressions,
    dwr.clicks      as clicks,
    dwr.mediaCost   as mediacost, -- fee
    dwr.sspRevenue  as ssprevenue,
    dwr.clearPrice  as clearprice,
    dwr.l_time      as l_time,
    dwr.b_date      as b_date,

    d.name          as dspname,
    p.amId          as amid,
    am.name         as amname,
    p.name          as publishername,
    a.name          as appname,
    c.name          as countryname,
    cr.name         as carriername
from dsp_traffic_dwr dwr
left join dsp d on d.id = dwr.dspId
left join publisher p on  p.id = dwr.publisherId
left join employee am on am.id = p.amId
left join app a on a.id = dwr.appId
left join country c on c.id = dwr.countryId
left join carrier cr on cr.id = dwr.carrierId;


-- Greenplum
create table dsp_traffic_dm(
    publisherId    BIGINT,
    appId          BIGINT,
    dspId          BIGINT,
    countryId      BIGINT,
    carrierId      BIGINT,
    utcDay         VARCHAR(100),
    cstDay         VARCHAR(100),
    requests       BIGINT,
    sends          BIGINT,
    winNotices     BIGINT,
    impressions    BIGINT,
    clicks         BIGINT,
    mediaCost      DECIMAL(19,10), -- fee
    sspRevenue     DECIMAL(19,10),
    clearPrice     DECIMAL(19,10),  -- sendFee
    l_time         VARCHAR(100),
    b_date         VARCHAR(100),

    dspName        VARCHAR(200),
    amId           INT,
    amName         VARCHAR(200),
    publisherName  VARCHAR(200),
    appName        VARCHAR(200),
    countryName    VARCHAR(200),
    carrierName    VARCHAR(200)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='dsp_traffic_dm_default_partition')
);


drop view if exists dsp_traffic_dm;
create view dsp_traffic_dm as
select
    dwr.*,
    concat_ws('^', d.name, cast(dwr.dspId as string) ) as dspName,
    p.amId as amId,
    concat_ws('^', am.name, cast(p.amId as string) )  as amName,
    concat_ws('^', p.name, cast(dwr.publisherId as string) ) as publisherName,
    concat_ws('^', a.name, cast(dwr.appId as string) ) as appName,
    concat_ws('^', c.name, cast(dwr.countryId as string) ) as countryName,
    concat_ws('^', cr.name, cast(dwr.carrierId as string) ) as  carrierName
from dsp_traffic_dwr dwr
left join dsp d on d.id = dwr.dspId
left join publisher p on  p.id = dwr.publisherId
left join employee am on am.id = p.amId
left join app a on a.id = dwr.appId
left join country c on c.id = dwr.countryId
left join carrier cr on cr.id = dwr.carrierId;


create table dsp(
   id   INT,
   name STRING
)
STORED AS ORC;

insert overwrite table dsp
select 1, "zephyr-digital" from dual
union all
select 2, "adacts" from dual
union all
select 3, "tappx" from dual
union all
select 4, "kds-media" from dual
union all
select 5, "clickky" from dual
union all
select 6, "howto5" from dual
union all
select 7, "vashoot" from dual;


create 'ssp_dsp_dwi_uuid_stat', 'f'

-- Order: dwrGroupBy b_date aggExprsAlias l_time


