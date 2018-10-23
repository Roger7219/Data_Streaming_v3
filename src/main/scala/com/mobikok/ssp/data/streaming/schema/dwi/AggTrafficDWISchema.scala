package com.mobikok.ssp.data.streaming.schema.dwi

import org.apache.spark.sql.types._

class AggTrafficDWISchema {
  def structType = AggTrafficDWISchema.structType
}

object AggTrafficDWISchema {
  val structType = StructType(
      StructField("repeats",      IntegerType) :: // 重复次数
      StructField("rowkey",       StringType) ::  // 用于去重

      StructField("appId",       IntegerType) ::  //ssp平台申请的id
      StructField("jarId",       IntegerType) ::  //下发jar id
      StructField("jarIds",      ArrayType(IntegerType))  :: //客户端存在jar列表
      StructField("publisherId", IntegerType) :: //渠道id
      StructField("imei",        StringType)  ::
      StructField("imsi",        StringType)  ::
      StructField("version",     StringType)  :: //手机系统版本号
      StructField("model",       StringType)  :: //机型
      StructField("screen",      StringType)  :: //分辨率
      StructField("installType", IntegerType) :: //安装区域
      StructField("sv",          StringType)  :: //版本号
      StructField("leftSize",    StringType)  :: //剩余存储空间
      StructField("androidId",   StringType)  :: //android id
      StructField("userAgent",   StringType)  :: //浏览器ua
      StructField("connectType", IntegerType) :: //网络类型
      StructField("createTime",  StringType)  :: //创建时间 + 请求时间？
      StructField("clickTime",   StringType)  :: //点击时间 +
      StructField("showTime",    StringType)  :: //展示时间 +
      StructField("reportTime",  StringType)  :: //报告时间
      StructField("countryId",   IntegerType) :: //国家id
      StructField("carrierId",   IntegerType) :: //运营商id
      StructField("ipAddr",      StringType)  :: //ip地址
      StructField("deviceType",  IntegerType) :: //设备类型
      StructField("pkgName",     StringType)  :: //包名
      StructField("s1",          StringType)  :: //透传参数1
      StructField("s2",          StringType)  :: //透传参数2
      StructField("clickId",     StringType)  :: //唯一标识
      StructField("reportPrice", DoubleType)  :: //计费金额
      StructField("pos",         IntegerType) ::
      StructField("affSub",      StringType) ::

      StructField("repeated",    StringType)  ::
      StructField("l_time",      StringType)  ::
      StructField("b_date",      StringType)  ::
      Nil
  )
}