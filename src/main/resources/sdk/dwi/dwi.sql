CREATE TABLE sdk_dyn_traffic_dwi(
  repeats       int         ,
  rowkey        string      ,

  timestamp     bigint      ,
  adv           int         ,
  adv_bd        int         ,
  jar           string      ,
  pub           int         ,
  pub_bd        int         ,
  app           int         ,
  country       string      ,
  model         string      ,
  brand         string      ,
  version       string      ,
  requests      bigint      ,
  responses     bigint      ,
  loads         bigint      ,
  success_loads bigint      ,
  revenue       double      ,
  cost          double
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

CREATE TABLE sdk_dyn_download_dwi(
  repeats           int   ,
  rowkey            string,

  timestamp         bigint,
  imei              string,
  app               int   ,
  jar               string,
  country           string,
  model             string,
  brand             string,
  version           string,
  downloads         bigint,
  success_downloads bigint
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;


CREATE TABLE sdk_dyn_user_active_dwi(
  repeats       int   ,
  rowkey        string,

  timestamp     bigint,
  appid         int   ,
  imei          String,
  model         String, -- 机型
  brand         String, -- 品牌
  osversion     String, -- 手机系统版本号
  screen        String, --手机屏幕尺寸(320x480)
  sdkversion    String, --sdk版本号
  installtype   String, --0数据区，1系统区
  leftsize      String, --剩余空间大小（M）
  ip            String, --用于国家解析
  country       String, --国家
  ctype         String  --网络连接类型
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;


CREATE TABLE sdk_dyn_user_new_dwi(
  repeats       int   ,
  rowkey        string,

  timestamp     bigint,
  appid         int   ,
  imei          String,
  model         String, -- 机型
  brand         String, -- 品牌
  osversion     String, -- 手机系统版本号
  screen        String, --手机屏幕尺寸(320x480)
  sdkversion    String, --sdk版本号
  installtype   String, --0数据区，1系统区
  leftsize      String, --剩余空间大小（M）
  ip            String, --用于国家解析
  country       String, --国家
  ctype         String  --网络连接类型
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;
