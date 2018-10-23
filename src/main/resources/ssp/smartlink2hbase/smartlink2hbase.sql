
CREATE TABLE SMART_DATA_VO_DWI_PHOENIX(
    "repeats"         INTEGER,
    "rowkey"          VARCHAR,
    "id"              INTEGER,
    "campaignId"      INTEGER,
    "s1"              VARCHAR,
    "s2"              VARCHAR,
    "createTime"      VARCHAR,
    "offerId"         INTEGER,
    "countryId"       INTEGER,
    "carrierId"       INTEGER,
    "deviceType"      INTEGER,
    "userAgent"       VARCHAR,
    "ipAddr"          VARCHAR,
    "clickId"         VARCHAR,
    "price"           VARCHAR,
    "status"          INTEGER,
    "sendStatus"      INTEGER,
    "reportTime"      VARCHAR,
    "sendPrice"       VARCHAR,
    "frameId"         INTEGER,
    "referer"         VARCHAR,
    "isTest"          INTEGER,
    "times"           INTEGER,
    "res"             VARCHAR,
    "type"            VARCHAR,
    "clickUrl"        VARCHAR,

    "repeated"        VARCHAR,
    "l_time"          VARCHAR,
    "b_date"          VARCHAR
    constraint pk primary key ("clickId")
);



