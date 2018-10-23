-------------------------------------------------------------
原始用户数据
-------------------------------------------------------------
drop table if exists ssp_user_V2_dwi;
create table ssp_user_V2_dwi(
    repeats int,
    rowkey string,
    imei string,
    imsi string,
    createTime string,
    activeTime string,
    appId int,
    model string,
    version string,
    sdkVersion int,
    installType int,
    leftSize string,
    androidId string,
    userAgent string,
    ipAddr string,
    screen string,
    countryId int,
    carrierId int,
    sv string,
    affSub string,
    lat string,
    lon string,
    mac1 string,
    mac2 string,
    ssid string,
    lac int,
    cellId int,
    ctype int
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING, b_time STRING)
STORED AS ORC;


--------------hbase-------------
create 'SSP_USERID_HISTORY', 'f', SPLITS => ['04', '08', '12', '16', '24', '28', '32', '36', '40', '44', '48', '52', '56', '60', '64', '68', '72', '76', '80', '84', '88', '92', '96', '99']


drop table if exists ssp_info_dwi;
create table ssp_info_dwi(
    repeats         int,
    rowkey          string,
    appId           int,
    imei            string,
    packageName     string,
    createTime      string
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;