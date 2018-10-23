-------------------------------------------------------------
-- adx kafka原始数据
-------------------------------------------------------------
CREATE TABLE backup__adx_dsp_dwi(
    repeats      INT,
    rowkey       STRING,
    `timestamp`    BIGINT,
    event_type   STRING,
    event_key    STRING,
    request      STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : INT,    -- PUBLISHER唯一ID
                         app_id        : INT,    -- APP唯一ID
                         country_id    : INT,    -- 国家ID
                         carrier_id    : INT,    -- 运营商ID
                         ad_id         : INT,    -- 广告位id
                         ad_type       : INT,    -- 广告类型
                         width         : INT,    -- 宽
                         height        : INT,    -- 高
                         iab1          : STRING, -- iab1
                         iab2          : STRING, -- iab2
                         ip            : STRING, -- IP地址
                         raw_request   : STRING  -- 请求内容
                     >,
                     trade : STRUCT<
                         deal_type     : STRING,  -- 交易类型
                         dsp_id        : BIGINT,  -- DSP唯一ID, winner
                         status        : INT,     -- 是否有返回
                         is_show       : INT,     -- 是否显示
                         bid_price     : DOUBLE,  -- 竞价
                         clear_price   : DOUBLE,  -- 返回内容
                         media_price   : DOUBLE,  -- 渠道竞价
                         win_noticeUrl : STRING,  -- 中签URL
                         click_url     : STRING,  -- 点击URL
                         pixel_url     : STRING,  -- pixelUrl
                         raw_response  : STRING   -- 返回内容
                     >
                 >,
    send         STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                          publisher_id  : INT,    -- PUBLISHER唯一ID
                          app_id        : INT,    -- APP唯一ID
                          country_id    : INT,    -- 国家ID
                          carrier_id    : INT,    -- 运营商ID
                          ad_id         : INT,    -- 广告位id
                          ad_type       : INT,    -- 广告类型
                          width         : INT,    -- 宽
                          height        : INT,    -- 高
                          iab1          : STRING, -- iab1
                          iab2          : STRING, -- iab2
                          ip            : STRING, -- IP地址
                          raw_request   : STRING  -- 请求内容
                     >,
                     trade : STRUCT<
                          deal_type     : STRING,  -- 交易类型
                          dsp_id        : BIGINT,  -- DSP唯一ID, winner
                          status        : INT,     -- 是否有返回
                          is_show       : INT,     -- 是否显示
                          bid_price     : DOUBLE,  -- 竞价
                          clear_price   : DOUBLE,  -- 返回内容
                          media_price   : DOUBLE,  -- 渠道竞价
                          win_noticeUrl : STRING,  -- 中签URL
                          click_url     : STRING,  -- 点击URL
                          pixel_url     : STRING,  -- pixelUrl
                          raw_response  : STRING   -- 返回内容
                     >
                 >,
    win_notice  STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : INT,    -- PUBLISHER唯一ID
                         app_id        : INT,    -- APP唯一ID
                         country_id    : INT,    -- 国家ID
                         carrier_id    : INT,    -- 运营商ID
                         ad_id         : INT,    -- 广告位id
                         ad_type       : INT,    -- 广告类型
                         width         : INT,    -- 宽
                         height        : INT,    -- 高
                         iab1          : STRING, -- iab1
                         iab2          : STRING, -- iab2
                         ip            : STRING, -- IP地址
                         raw_request   : STRING  -- 请求内容
                     >,
                     trade : STRUCT<
                          deal_type     : STRING,  -- 交易类型
                          dsp_id        : BIGINT,  -- DSP唯一ID, winner
                          status        : INT,     -- 是否有返回
                          is_show       : INT,     -- 是否显示
                          bid_price     : DOUBLE,  -- 竞价
                          clear_price   : DOUBLE,  -- 返回内容
                          media_price   : DOUBLE,  -- 渠道竞价
                          win_noticeUrl : STRING,  -- 中签URL
                          click_url     : STRING,  -- 点击URL
                          pixel_url     : STRING,  -- pixelUrl
                          raw_response  : STRING   -- 返回内容
                     >
                 >,
    impression   STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : INT,    -- PUBLISHER唯一ID
                         app_id        : INT,    -- APP唯一ID
                         country_id    : INT,    -- 国家ID
                         carrier_id    : INT,    -- 运营商ID
                         ad_id         : INT,    -- 广告位id
                         ad_type       : INT,    -- 广告类型
                         width         : INT,    -- 宽
                         height        : INT,    -- 高
                         iab1          : STRING, -- iab1
                         iab2          : STRING, -- iab2
                         ip            : STRING, -- IP地址
                         raw_request   : STRING  -- 请求内容
                     >,
                     trade : STRUCT<
                          deal_type     : STRING,  -- 交易类型
                          dsp_id        : BIGINT,  -- DSP唯一ID, winner
                          status        : INT,     -- 是否有返回
                          is_show       : INT,     -- 是否显示
                          bid_price     : DOUBLE,  -- 竞价
                          clear_price   : DOUBLE,  -- 返回内容
                          media_price   : DOUBLE,  -- 渠道竞价
                          win_noticeUrl : STRING,  -- 中签URL
                          click_url     : STRING,  -- 点击URL
                          pixel_url     : STRING,  -- pixelUrl
                          raw_response  : STRING   -- 返回内容
                     >
                 >,
    click       STRUCT<
                     click_id : STRING,
                     request: STRUCT<
                         publisher_id  : INT,    -- PUBLISHER唯一ID
                         app_id        : INT,    -- APP唯一ID
                         country_id    : INT,    -- 国家ID
                         carrier_id    : INT,    -- 运营商ID
                         ad_id         : INT,    -- 广告位id
                         ad_type       : INT,    -- 广告类型
                         width         : INT,    -- 宽
                         height        : INT,    -- 高
                         iab1          : STRING, -- iab1
                         iab2          : STRING, -- iab2
                         ip            : STRING, -- IP地址
                         raw_request   : STRING  -- 请求内容
                     >,
                     trade : STRUCT<
                          deal_type     : STRING,  -- 交易类型
                          dsp_id        : BIGINT,  -- DSP唯一ID, winner
                          status        : INT,     -- 是否有返回
                          is_show       : INT,     -- 是否显示
                          bid_price     : DOUBLE,  -- 竞价
                          clear_price   : DOUBLE,  -- 返回内容
                          media_price   : DOUBLE,  -- 渠道竞价
                          win_noticeUrl : STRING,  -- 中签URL
                          click_url     : STRING,  -- 点击URL
                          pixel_url     : STRING,  -- pixelUrl
                          raw_response  : STRING   -- 返回内容
                     >
                 >
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;

create table backup__adx_ssp_dwi like backup__adx_dsp_dwi;

create table backup__adx_traffic_dwr(
    publisherId    INT,
    appId          INT,
    dspId          INT,
    countryId      INT,
    carrierId      INT,
    adId           INT,
    adType         INT,
    iab1           STRING,
    iab2           STRING,
    width          INT,
    height         INT,
    utcDay         STRING,
    utcHour        INT,

    sspRequests    BIGINT,    -- 请求数
    sspSends       BIGINT,    -- 响应数(下发数)

    dspRequests    BIGINT,    -- DSP 请求数
    dspSends       BIGINT,    -- DSP 响应数

    winNotices      BIGINT,          -- 中签数
    impressions     BIGINT,          -- 展示数
    clicks          BIGINT,          -- 点击数

    realImpressions BIGINT,         -- 真实的广告展示数
    realClicks      BIGINT,         -- 真实的点击数

    mediaPrice      DECIMAL(19,10), -- if(is_show == 1, 0, mediaPrice)

    realMediaPrice      DECIMAL(19,10), -- 真实的渠道收益
    realCpmMediaPrice  DECIMAL(19,10),
    realCpcMediaPrice  DECIMAL(19,10),

    clearPrice      DECIMAL(19,10),

    realClearPrice      DECIMAL(19,10), --
    realCpmClearPrice   DECIMAL(19,10),
    realCpcClearPrice   DECIMAL(19,10),

    profit          DECIMAL(19,10)  -- 净收益 realClearPrice - mediaPrice
)
PARTITIONED BY (l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;




-- Hive
drop view if exists adx_traffic_dm;
create view adx_traffic_dm as
select
    dwr.publisherId   as publisherid,
    dwr.appId         as appid,
    dwr.dspId         as dspid,
    dwr.countryId     as countryid,
    dwr.carrierId     as carrierid,
    dwr.adId          as adid,     --广告位id
    dwr.adType        as adtypeid, --广告类型, eg: banner
    dwr.iab1          as iab1id,
    dwr.iab2          as iab2id,
    dwr.width         as width,
    dwr.height        as height,
    dwr.utcDay        as utcday,
    dwr.utcHour       as utchour,

    dwr.sspRequests   as ssprequests,    -- 请求数
    dwr.sspSends      as sspsends,       -- 响应数(下发数)
    dwr.dspRequests   as dsprequests,    -- DSP 请求数
    dwr.dspSends      as dspsends,       -- DSP 响应数
    dwr.winNotices    as winnotices,
    dwr.impressions   as impressions,
    dwr.clicks        as clicks,
    dwr.realImpressions     as realimpressions,
    dwr.realClicks          as realclicks,
    dwr.mediaPrice          as mediaprice,
    dwr.realMediaPrice      as realmediaprice,
    dwr.realCpmMediaPrice   as realcpmmediaprice,
    dwr.realCpcMediaPrice   as realcpcmediaprice,

    dwr.clearPrice          as clearprice,
    dwr.realClearPrice      as realclearprice,
    dwr.realCpmClearPrice   as realcpmclearprice,
    dwr.realCpcClearPrice   as realcpcclearprice,
    dwr.profit        as profit,  -- clearprice - mediaPrice

    dwr.l_time        as l_time,
    dwr.b_date        as b_date,

    p.name            as publishername,
    a.name            as appname,
    d.name            as dspname,
    c.name            as countryname,
    cr.name           as carriername,
    aa.name           as adname,     -- 广告位名
    aat.name          as adtypename, -- eg: Banner
    ia1.iab1Name      as iab1name,
    ia1.iab2Name      as iab2name,
    --+
    p.amId            as amid,
    am.name           as amname,
    am.proxyId        as publisherProxyId
from adx_traffic_dwr dwr
left join dsp_info d on d.id = dwr.dspId
left join publisher p on  p.id = dwr.publisherId
left join employee am on am.id = p.amId
left join app a on a.id = dwr.appId
left join country c on c.id = dwr.countryId
left join carrier cr on cr.id = dwr.carrierId
-- +
left join adx_ad_type aat on aat.id = dwr.adType
left join app_ad aa on aa.id = dwr.adId
left join iab ia1 on ia1.iab1 = dwr.iab1 and ia1.iab2 = dwr.iab2
;

drop table if exists adx_ad_type;
CREATE TABLE adx_ad_type(
    id   int,
    name string
)
STORED AS ORC;

insert into table adx_ad_type values(1, "banner");

CREATE TABLE iab (
  iab1     STRING,
  iab2     STRING,
  iab1Name STRING,
  iab2Name STRING
)
STORED AS ORC;

CREATE TABLE app_ad (
    id       INT,
    name     STRING,
    appId    INT,
    adType   INT,
    width    INT,
    height   INT,
    iab1     STRING,
    iab2     STRING,
    bidPrice DOUBLE,
    token    STRING
)
STORED AS ORC;

CREATE TABLE dsp_info (
    id           INT,
    name         STRING
)
STORED AS ORC;


--
--drop view if exists adx_traffic_dm;
--create view dsp_traffic_dm as
--select
--    dwr.*,
--    d.name  as dspName,
--    p.amId  as amId,
--    am.name as amName,
--    p.name  as publisherName,
--    a.name  as appName,
--    c.name  as countryName,
--    cr.name as carrierName
--from dsp_traffic_dwr dwr
--left join dsp d on d.id = dwr.dspId
--left join publisher p on  p.id = dwr.publisherId
--left join employee am on am.id = p.amId
--left join app a on a.id = dwr.appId
--left join country c on c.id = dwr.countryId
--left join carrier cr on cr.id = dwr.carrierId;


create table dsp(
   id   INT,
   name STRING
)
STORED AS ORC;

--insert overwrite table dsp
--select 1, "zephyr-digital" from dual
--union all
--select 2, "adacts" from dual
--union all
--select 3, "tappx" from dual
--union all
--select 4, "kds-media" from dual
--union all
--select 5, "clickky" from dual
--union all
--select 6, "howto5" from dual
--union all
--select 7, "vashoot" from dual
--union all
--select 8, "Convertise" from dual;


disable 'backup__adx_dsp_dwi_uuid'
drop 'backup__adx_dsp_dwi_uuid'
create 'backup__adx_dsp_dwi_uuid', 'f'

disable 'backup__adx_ssp_dwi_uuid'
drop 'backup__adx_ssp_dwi_uuid'
create 'backup__adx_ssp_dwi_uuid', 'f'



disable 'adx_dsp_dwi_uuid'
drop 'adx_dsp_dwi_uuid'
create 'adx_dsp_dwi_uuid', 'f'

disable 'adx_ssp_dwi_uuid'
drop 'adx_ssp_dwi_uuid'
create 'adx_ssp_dwi_uuid', 'f'


------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists adx_traffic_dwr_view;
create view adx_traffic_dwr_view as
select
    publisherId,
    appId,
    dspId,
    countryId,
    carrierId,
    adId,
    adType,
    iab1,
    iab2,
    width,
    height,
    utcDay,
    utcHour,
    sum(sspRequests)       as sspRequests,
    sum(sspSends)          as sspSends,
    sum(dspRequests)       as dspRequests,
    sum(dspSends)          as dspSends,
    sum(winNotices)        as winNotices,
    sum(impressions)       as impressions,
    sum(clicks)            as clicks,
    sum(realImpressions)   as realImpressions,
    sum(realClicks)        as realClicks,
    sum(mediaPrice)        as mediaPrice,
    sum(realMediaPrice)    as realMediaPrice,
    sum(realCpmMediaPrice) as realCpmMediaPrice,
    sum(realCpcMediaPrice) as realCpcMediaPrice,
    sum(clearPrice)        as clearPrice,
    sum(realClearPrice)    as realClearPrice,
    sum(realCpmClearPrice) as realCpmClearPrice,
    sum(realCpcClearPrice) as realCpcClearPrice,
    sum(profit)            as profit,
    t0.l_time,
    t0.b_date
from(
    select
        repeated,
        event_type,
        request,
        send,
        win_notice,
        impression,
        click,
        `timestamp`,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.publisher_id
            WHEN 'SEND'       THEN send.request.publisher_id
            WHEN 'WINNOTICE'  THEN win_notice.request.publisher_id
            WHEN 'IMPRESSION' THEN impression.request.publisher_id
            WHEN 'CLICK'      THEN click.request.publisher_id
            END as publisherId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.app_id
            WHEN 'SEND'       THEN send.request.app_id
            WHEN 'WINNOTICE'  THEN win_notice.request.app_id
            WHEN 'IMPRESSION' THEN impression.request.app_id
            WHEN 'CLICK'      THEN click.request.app_id
            END as appId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.trade.dsp_id
            WHEN 'SEND'       THEN send.trade.dsp_id
            WHEN 'WINNOTICE'  THEN win_notice.trade.dsp_id
            WHEN 'IMPRESSION' THEN impression.trade.dsp_id
            WHEN 'CLICK'      THEN click.trade.dsp_id
            END as dspId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.country_id
            WHEN 'SEND'       THEN send.request.country_id
            WHEN 'WINNOTICE'  THEN win_notice.request.country_id
            WHEN 'IMPRESSION' THEN impression.request.country_id
            WHEN 'CLICK'      THEN click.request.country_id
            END as countryId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.carrier_id
            WHEN 'SEND'       THEN send.request.carrier_id
            WHEN 'WINNOTICE'  THEN win_notice.request.carrier_id
            WHEN 'IMPRESSION' THEN impression.request.carrier_id
            WHEN 'CLICK'      THEN click.request.publisher_id
            END as carrierId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.ad_id
            WHEN 'SEND'       THEN send.request.ad_id
            WHEN 'WINNOTICE'  THEN win_notice.request.ad_id
            WHEN 'IMPRESSION' THEN impression.request.ad_id
            WHEN 'CLICK'      THEN click.request.ad_id
            END as adId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.ad_type
            WHEN 'SEND'       THEN send.request.ad_type
            WHEN 'WINNOTICE'  THEN win_notice.request.ad_type
            WHEN 'IMPRESSION' THEN impression.request.ad_type
            WHEN 'CLICK'      THEN click.request.ad_type
            END as adType,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.iab1
            WHEN 'SEND'       THEN send.request.iab1
            WHEN 'WINNOTICE'  THEN win_notice.request.iab1
            WHEN 'IMPRESSION' THEN impression.request.iab1
            WHEN 'CLICK'      THEN click.request.iab1
            END as iab1,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.iab2
            WHEN 'SEND'       THEN send.request.iab2
            WHEN 'WINNOTICE'  THEN win_notice.request.iab2
            WHEN 'IMPRESSION' THEN impression.request.iab2
            WHEN 'CLICK'      THEN click.request.iab2
            END as iab2,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.width
            WHEN 'SEND'       THEN send.request.width
            WHEN 'WINNOTICE'  THEN win_notice.request.width
            WHEN 'IMPRESSION' THEN impression.request.width
            WHEN 'CLICK'      THEN click.request.width
            END as width,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.height
            WHEN 'SEND'       THEN send.request.height
            WHEN 'WINNOTICE'  THEN win_notice.request.height
            WHEN 'IMPRESSION' THEN impression.request.height
            WHEN 'CLICK'      THEN click.request.height
            END as height,
        from_unixtime(floor(`timestamp`/1000), 'yyyy-MM-dd') as utcDay,
        from_unixtime(floor(`timestamp`/1000), 'HH') as utcHour,
        0 as sspRequests,
        0 as sspSends,
        CASE event_type WHEN 'REQUEST'    THEN 1 ELSE 0 END as dspRequests,
        CASE event_type WHEN 'REQUEST'    THEN if(request.trade.status = 1, 1, 0) ELSE 0 END as dspSends, -- dsp responses
        0 as winNotices,
        0 as impressions,
        0 as clicks,
        0 as realImpressions,
        0 as realClicks,
        0 as mediaPrice,
        0 as realMediaPrice,
        0 as realCpmMediaPrice,
        0 as realCpcMediaPrice,
        0 as clearPrice,
        0 as realClearPrice,
        0 as realCpmClearPrice,
        0 as realCpcClearPrice,
        0 as profit,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from adx_dsp_dwi
    UNION ALL

    select
        repeated,
        event_type,
        request,
        send,
        win_notice,
        impression,
        click,
        `timestamp`,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.publisher_id
            WHEN 'SEND'       THEN send.request.publisher_id
            WHEN 'WINNOTICE'  THEN win_notice.request.publisher_id
            WHEN 'IMPRESSION' THEN impression.request.publisher_id
            WHEN 'CLICK'      THEN click.request.publisher_id
            END as publisherId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.app_id
            WHEN 'SEND'       THEN send.request.app_id
            WHEN 'WINNOTICE'  THEN win_notice.request.app_id
            WHEN 'IMPRESSION' THEN impression.request.app_id
            WHEN 'CLICK'      THEN click.request.app_id
            END as appId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.trade.dsp_id
            WHEN 'SEND'       THEN send.trade.dsp_id
            WHEN 'WINNOTICE'  THEN win_notice.trade.dsp_id
            WHEN 'IMPRESSION' THEN impression.trade.dsp_id
            WHEN 'CLICK'      THEN click.trade.dsp_id
            END as dspId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.country_id
            WHEN 'SEND'       THEN send.request.country_id
            WHEN 'WINNOTICE'  THEN win_notice.request.country_id
            WHEN 'IMPRESSION' THEN impression.request.country_id
            WHEN 'CLICK'      THEN click.request.country_id
            END as countryId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.carrier_id
            WHEN 'SEND'       THEN send.request.carrier_id
            WHEN 'WINNOTICE'  THEN win_notice.request.carrier_id
            WHEN 'IMPRESSION' THEN impression.request.carrier_id
            WHEN 'CLICK'      THEN click.request.publisher_id
            END as carrierId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.ad_id
            WHEN 'SEND'       THEN send.request.ad_id
            WHEN 'WINNOTICE'  THEN win_notice.request.ad_id
            WHEN 'IMPRESSION' THEN impression.request.ad_id
            WHEN 'CLICK'      THEN click.request.ad_id
            END as adId,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.ad_type
            WHEN 'SEND'       THEN send.request.ad_type
            WHEN 'WINNOTICE'  THEN win_notice.request.ad_type
            WHEN 'IMPRESSION' THEN impression.request.ad_type
            WHEN 'CLICK'      THEN click.request.ad_type
            END as adType,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.iab1
            WHEN 'SEND'       THEN send.request.iab1
            WHEN 'WINNOTICE'  THEN win_notice.request.iab1
            WHEN 'IMPRESSION' THEN impression.request.iab1
            WHEN 'CLICK'      THEN click.request.iab1
            END as iab1,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.iab2
            WHEN 'SEND'       THEN send.request.iab2
            WHEN 'WINNOTICE'  THEN win_notice.request.iab2
            WHEN 'IMPRESSION' THEN impression.request.iab2
            WHEN 'CLICK'      THEN click.request.iab2
            END as iab2,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.width
            WHEN 'SEND'       THEN send.request.width
            WHEN 'WINNOTICE'  THEN win_notice.request.width
            WHEN 'IMPRESSION' THEN impression.request.width
            WHEN 'CLICK'      THEN click.request.width
            END as width,
        CASE event_type
            WHEN 'REQUEST'    THEN request.request.height
            WHEN 'SEND'       THEN send.request.height
            WHEN 'WINNOTICE'  THEN win_notice.request.height
            WHEN 'IMPRESSION' THEN impression.request.height
            WHEN 'CLICK'      THEN click.request.height
            END as height,
        from_unixtime(floor(`timestamp`/1000), 'yyyy-MM-dd') as utcDay,
        from_unixtime(floor(`timestamp`/1000), 'HH') as utcHour,
        CASE event_type WHEN 'REQUEST'    THEN 1 ELSE 0 END as sspRequests,
        CASE event_type WHEN 'SEND'       THEN 1 ELSE 0 END as sspSends,
        0 as dspRequests,
        0 as dspSends,
        CASE event_type WHEN 'WINNOTICE'   THEN 1 ELSE 0 END as winNotices,
        CASE event_type WHEN 'IMPRESSION' THEN if(impression.trade.is_show = 1, 0, 1) ELSE 0 END as impressions,
        CASE event_type WHEN 'CLICK' THEN if(click.trade.is_show = 1, 0, 1) ELSE 0 END as clicks,
        CASE event_type WHEN 'IMPRESSION'  THEN 1 ELSE 0 END as realImpressions,
        CASE event_type WHEN 'CLICK'  THEN 1 ELSE 0 END as realClicks,
        CASE event_type
                WHEN 'IMPRESSION' THEN
                    if(impression.trade.is_show = 1, 0, CASE impression.trade.deal_type
                        WHEN 'CPM' THEN CAST(impression.trade.media_price AS DECIMAL(19,10))/1000
                        ELSE 0
                        END)
                WHEN 'CLICK' THEN
                    if(click.trade.is_show = 1, 0, CASE click.trade.deal_type
                        WHEN 'CPC' THEN click.trade.media_price
                        ELSE 0
                        END)
                END as mediaPrice,
        CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.media_price AS DECIMAL(19,10))/1000
                      ELSE 0
                      END
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.media_price
                      ELSE 0
                      END
                  END as realMediaPrice,
        CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.media_price AS DECIMAL(19,10))/1000
                      END
                  END as realCpmMediaPrice,
        CASE event_type
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.media_price
                      END
                  END as realCpcMediaPrice,
        CASE event_type
                WHEN 'IMPRESSION' THEN
                    if(impression.trade.is_show = 1, 0 , CASE impression.trade.deal_type
                        WHEN 'CPM' THEN CAST(impression.trade.clear_price AS DECIMAL(19,10))/1000
                        ELSE 0 END)
                WHEN 'CLICK' THEN
                    if(click.trade.is_show = 1, 0, CASE click.trade.deal_type
                        WHEN 'CPC' THEN click.trade.clear_price
                        ELSE 0
                        END)
                END as clearPrice,
        CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.clear_price AS DECIMAL(19,10))/1000
                      ELSE 0 END
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.clear_price
                      ELSE 0
                      END
                  END as realClearPrice,
        CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.clear_price AS DECIMAL(19,10))/1000
                      END
                  END as realCpmClearPrice,
        CASE event_type
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.clear_price
                      END
                  END as realCpcClearPrice,
        CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.clear_price AS DECIMAL(19,10))/1000
                      ELSE 0
                      END
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.clear_price
                      ELSE 0
                      END
                  END
                  -
                  CASE event_type
                  WHEN 'IMPRESSION' THEN CASE impression.trade.deal_type
                      WHEN 'CPM' THEN CAST(impression.trade.media_price AS DECIMAL(19,10))/1000
                      ELSE 0
                      END
                  WHEN 'CLICK' THEN CASE click.trade.deal_type
                      WHEN 'CPC' THEN click.trade.media_price
                      ELSE 0
                      END
                  END as profit,
        date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
        b_date
    from adx_ssp_dwi
)t0
where repeated ='N'
group by
    publisherId,
    appId,
    dspId,
    countryId,
    carrierId,
    adId,
    adType,
    iab1,
    iab2,
    width,
    height,
    utcDay,
    utcHour,
    l_time,
    b_date;