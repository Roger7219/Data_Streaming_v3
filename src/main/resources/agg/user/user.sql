CREATE TABLE agg_user_dwi(
    repeats     INT,
    rowkey      STRING,
    appId       INT,     --ssp平台申请的id
    jarId       INT,     --sdk客户id
    jarIds      array<INT>,
    publisherId INT,     --渠道id
    imei        STRING,
    imsi        STRING,
    version     STRING,  --手机系统版本号
    model       STRING,  --机型

    screen      STRING,  --分辨率
    installType INT,     --安装区域
    sv          STRING,  --版本号
    leftSize    STRING,  --剩余存储空间
    androidId   STRING,  --android id
    userAgent   STRING,  --浏览器ua
    connectType INT,     --网络类型
    createTime  STRING,  --创建时间
    countryId   INT,     --国家id
    carrierId   INT,     --运营商id
    ipAddr      STRING,  --ip地址
    deviceType  INT,     --设备类型
    pkgName     STRING   --包名
    affSub      STRING   --子渠道
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

-- 新增用户数
CREATE TABLE agg_user_new_dwr(
    jarId       INT,
    appId       INT,
    countryId   INT,
    carrierId   INT,
    connectType INT,
    publisherId INT,
    affSub      STRING,   --子渠道
    newCount    BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

-- 活跃用户数
CREATE TABLE agg_user_active_dwr(
    jarId       INT,
    appId       INT,
    countryId   INT,
    carrierId   INT,
    connectType INT,
    publisherId INT,
    affSub      STRING,   --子渠道
    activeCount BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


create 'agg_user_new_dwi__uuid', 'f'
create 'agg_user_active_dwi__uuid', 'f'

create 'agg_user_active_dwi__day_uuid', 'f'
