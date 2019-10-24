package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class SdkDynLoadingUserDWISchema{
  def structType = SdkDynLoadingUserDWISchema.structType
}
// 用户明细
object SdkDynLoadingUserDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("timestamp",   LongType)    ::
    StructField("appid",       IntegerType) ::
    StructField("imei",        StringType)  ::
    StructField("model",       StringType)  :: //机型
    StructField("brand",       StringType)  :: //品牌
    StructField("osversion",   StringType)  :: //手机系统版本号
    StructField("screen",      StringType)  :: //手机屏幕尺寸(320x480)
    StructField("sdkversion",  StringType)  :: //sdk版本号
    StructField("installtype", StringType)  :: //0数据区，1系统区
    StructField("leftsize",    StringType)  :: //剩余空间大小（M）
    StructField("ip",          StringType)  :: //用于国家解析
    StructField("country",     StringType)  :: //国家解析
    StructField("ctype",       StringType)  :: //网络连接类型
    Nil)

}
