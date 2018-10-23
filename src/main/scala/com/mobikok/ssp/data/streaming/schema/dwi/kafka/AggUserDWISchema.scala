package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/6/12.
  */
class AggUserDWISchema {
  def structType = AggUserDWISchema.structType
}
object AggUserDWISchema{

  val structType = StructType(
    StructField("appId",       IntegerType) :: //ssp平台申请的id
    StructField("jarId",       IntegerType) :: //sdk客户id
    StructField("jarIds",      ArrayType(IntegerType)) ::
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
    StructField("createTime",  StringType)  :: //创建时间
    StructField("countryId",   IntegerType) :: //国家id
    StructField("carrierId",   IntegerType) :: //运营商id
    StructField("ipAddr",      StringType)  :: //ip地址
    StructField("deviceType",  IntegerType) :: //设备类型
    StructField("pkgName",     StringType)  :: //包名
    StructField("affSub",      StringType)  :: //子渠道
    Nil
  )
}