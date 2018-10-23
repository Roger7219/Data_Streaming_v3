-- 添加列
ALTER TABLE SSP_SEND_DWI_PHOENIX ADD "affSub" VARCHAR;
--add s3-s5
ALTER TABLE SSP_SEND_DWI_PHOENIX ADD "s3" VARCHAR;
ALTER TABLE SSP_SEND_DWI_PHOENIX ADD "s4" VARCHAR;
ALTER TABLE SSP_SEND_DWI_PHOENIX ADD "s5" VARCHAR;

-- 修改列
alter table SSP_SEND_DWI_PHOENIX change column  affSub "affSub" VARCHAR
alter table SSP_SEND_DWI_PHOENIX drop column "affSub"




CREATE TABLE SSP_SEND_DWI_PHOENIX_20180105(
    "repeats"     INTEGER   ,
    "rowkey"      VARCHAR,

    "id"          INTEGER   ,
    "publisherId" INTEGER   , -- + AM
    "subId"       INTEGER   , -- + APP
    "offerId"     INTEGER   ,
    "campaignId"  INTEGER   , -- + campaign > 上级ADVER
    "countryId"   INTEGER   ,
    "carrierId"   INTEGER   ,
    "deviceType"  INTEGER   ,
    "userAgent"   VARCHAR,
    "ipAddr"      VARCHAR,
    "clickId"     VARCHAR,
    "price"       VARCHAR, --DOUBLE
    "reportTime"  VARCHAR,
    "createTime"  VARCHAR,
    "clickTime"   VARCHAR,
    "showTime"    VARCHAR,
    "requestType" VARCHAR,
    "priceMethod" INTEGER   ,  -- 1 cpc(click), 2 cpm(show), 3 cpa(转化数=计费数?)
    "bidPrice"    VARCHAR, -- DOUBLE
    "adType"      INTEGER   ,
    "isSend"      INTEGER   ,
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
    "s3"          VARCHAR,
    "s4"          VARCHAR,
    "s5"          VARCHAR,

    "repeated" VARCHAR,
    "l_time"   VARCHAR,
    "b_date"   VARCHAR
    constraint pk primary key ("clickId")
);

-- 设置ttl为一个月,60*60*24*30
-- dwi.phoenix.ttl=2592000
disable 'SSP_SEND_DWI_PHOENIX_20180105'
-- 10天
alter 'SSP_SEND_DWI_PHOENIX_20180105',{NAME=>'0',TTL=>'864000'}
alter 'SSP_SEND_DWI_PHOENIX_20180105', NAME => '0', COMPRESSION => 'snappy'
alter 'SSP_SEND_DWI_PHOENIX_20180105', BLOOMFILTER => 'ROW'


enable 'SSP_SEND_DWI_PHOENIX_20180105'















---------------------------------------------------------------------------------------------------
-- b_date 修复
select count(1)
from ssp_send_dwi
where l_time >= "2017-08-28"
-- 484038742


insert  overwrite table ssp_send_dwi partition(repeated, l_time,b_date)
select 
 repeats                   ,
 rowkey                    ,     
 id                        ,
 publisherid               ,
 subid                     ,
 offerid                   ,
 campaignid                ,
 countryid                 ,
 carrierid                 ,
 devicetype                ,
 useragent                 ,     
 ipaddr                    ,     
 clickid                   ,     
 price                     ,      
 reporttime                ,     
 createtime                ,     
 clicktime                 ,     
 showtime                  ,     
 requesttype               ,     
 pricemethod               ,
 bidprice                  ,      
 adtype                    ,
 issend                    ,
 reportprice               ,      
 sendprice                 ,      
 s1                        ,     
 s2                        ,     
 gaid                      ,     
 androidid                 ,     
 idfa                      ,     
 postback                  ,     
 sendstatus                ,
 sendtime                  ,     
 sv                        ,     
 imei                      ,     
 imsi                      ,     
 imageid                   ,
 repeated                  ,     
 l_time                    ,     
 to_date(createtime) as b_date

from ssp_send_dwi
where l_time >= "2017-08-28"



