--外部數據存儲
CREATE TABLE smart_data_vo_dwi(
        repeats         INT,
        rowkey          STRING,
        id              int,
        campaignId      int,
        s1              STRING,
        s2              STRING,
        createTime      STRING,
        offerId         int,
        countryId       int,
        carrierId       int,
        deviceType      int,
        userAgent       STRING,
        ipAddr          STRING,
        clickId         STRING,
        price           DOUBLE,
        status          int,
        sendStatus      int,
        reportTime      STRING,
        sendPrice       DOUBLE,
        frameId         int,
        referer         STRING,
        isTest          int,
        times           int,
        res             STRING,
        type            STRING,
        clickUrl        STRING,
        reportIp        STRING
)PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;



ALTER TABLE smart_data_vo_dwi ADD COLUMNS (reportIp String);