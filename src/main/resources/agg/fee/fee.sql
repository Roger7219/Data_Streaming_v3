CREATE TABLE agg_fee_dwi(
    repeats     INT,
    rowkey      STRING,

    appId       INT,        --ssp平台申请的id
    jarId       INT,        --下发jar id
    jarIds      ARRAY<INT>, --客户端存在jar列表
    publisherId INT,        --渠道id
    imei        STRING,
    imsi        STRING,
    version     STRING,     --手机系统版本号
    model       STRING,     --机型
    screen      STRING,     --分辨率
    installType INT,         --安装区域
    sv          STRING,     --版本号
    leftSize    STRING,     --剩余存储空间
    androidId   STRING,     --android id
    userAgent   STRING,     --浏览器ua
    connectType INT,         --网络类型
    createTime  STRING,     --创建时间 + 请求时间？
    clickTime   STRING,     --点击时间 +
    showTime    STRING,     --展示时间 +
    reportTime  STRING,     --报告时间
    countryId   INT,         --国家id
    carrierId   INT,         --运营商id
    ipAddr      STRING,     --ip地址
    deviceType  INT,         --设备类型
    pkgName     STRING,     --包名
    s1          STRING,     --透传参数1
    s2          STRING,     --透传参数2
    clickId     STRING,     --唯一标识
    reportPrice DOUBLE,      --计费金额
    pos     INT,
    affSub      STRING,   --子渠道
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

CREATE TABLE agg_fee_unjoin_dwi(
    repeats     INT,
    rowkey      STRING,

    appId       INT,        --ssp平台申请的id
    jarId       INT,        --下发jar id
    jarIds      ARRAY<INT>, --客户端存在jar列表
    publisherId INT,        --渠道id
    imei        STRING,
    imsi        STRING,
    version     STRING,     --手机系统版本号
    model       STRING,     --机型
    screen      STRING,     --分辨率
    installType INT,         --安装区域
    sv          STRING,     --版本号
    leftSize    STRING,     --剩余存储空间
    androidId   STRING,     --android id
    userAgent   STRING,     --浏览器ua
    connectType INT,         --网络类型
    createTime  STRING,     --创建时间 + 请求时间？
    clickTime   STRING,     --点击时间 +
    showTime    STRING,     --展示时间 +
    reportTime  STRING,     --报告时间
    countryId   INT,         --国家id
    carrierId   INT,         --运营商id
    ipAddr      STRING,     --ip地址
    deviceType  INT,         --设备类型
    pkgName     STRING,     --包名
    s1          STRING,     --透传参数1
    s2          STRING,     --透传参数2
    clickId     STRING,     --唯一标识
    reportPrice DOUBLE,      --计费金额
    pos         INT,
    affSub      STRING,   --子渠道
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;


CREATE TABLE agg_fee_dwr(
    jarId       INT,
    appId       INT,
    countryId   INT,
    carrierId   INT,
    connectType INT,
    publisherId INT,
    affSub      STRING,   --子渠道
    cost        DOUBLE
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;