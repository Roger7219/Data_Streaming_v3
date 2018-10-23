
CREATE TABLE SSP_OVERALL_POSTBACK_DWI_PHOENIX(
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
    "packagename" VARCHAR,
    "domain"      VARCHAR,
    "respstatus"  INTEGER,
    "winprice"    VARCHAR,-- DOUBLE
    "wintime"     VARCHAR,
    "appprice"    VARCHAR,-- DOUBLE
    "test"        INTEGER,
    "ruleid"      INTEGER,
    "smartid"     INTEGER,
    "pricepercent" INTEGER,
    "apppercent"  INTEGER,
    "salepercent" INTEGER,
    "appsalepercent" INTEGER,
    "reportip"    VARCHAR,
    "eventname"   VARCHAR,
    "eventvalue"  INTEGER,
    "refer"       VARCHAR,
    "status"      INTEGER,
    "region"      VARCHAR,
    "city"        VARCHAR,

    "repeated"    VARCHAR,
    "l_time"      VARCHAR,
    "b_date"      VARCHAR,
    "b_time"      VARCHAR
    constraint pk primary key ("clickId")
);

-- 设置ttl为一个月,60*60*24*30
-- dwi.phoenix.ttl=2592000
disable 'SSP_OVERALL_POSTBACK_DWI_PHOENIX'
-- 7天
alter 'SSP_OVERALL_POSTBACK_DWI_PHOENIX',{NAME=>'0',TTL=>'604800'}
alter 'SSP_OVERALL_POSTBACK_DWI_PHOENIX', NAME => '0', COMPRESSION => 'snappy'


enable 'SSP_OVERALL_POSTBACK_DWI_PHOENIX'

