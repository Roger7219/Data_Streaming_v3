package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._


class SspTrafficOldDWISchema{
  def structType = SspTrafficOldDWISchema.structType
}
object SspTrafficOldDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("id",          IntegerType) ::
      StructField("publisherId", IntegerType) ::
      StructField("subId",       IntegerType) ::
      StructField("offerId",     IntegerType) ::
      StructField("campaignId",  IntegerType) ::
      StructField("countryId",   IntegerType) ::
      StructField("carrierId",   IntegerType) ::
      StructField("deviceType",  IntegerType) ::
      StructField("userAgent",   StringType)  ::
      StructField("ipAddr",      StringType)  ::
      StructField("clickId",     StringType)  ::
      StructField("price",       DoubleType)  ::
      StructField("reportTime",  StringType)  :: //计费数据的时间
      StructField("createTime",  StringType)  ::
      StructField("clickTime",   StringType)  ::
      StructField("showTime",    StringType)  ::
      StructField("requestType", StringType)  ::
      StructField("priceMethod", IntegerType) ::
      StructField("bidPrice",    DoubleType)  ::
      StructField("adType",      IntegerType) ::
      StructField("isSend",      IntegerType) ::
      StructField("reportPrice", DoubleType)  ::
      StructField("sendPrice",   DoubleType)  ::
      StructField("s1",          StringType)  ::
      StructField("s2",          StringType)  ::
      StructField("gaid",        StringType)  ::
      StructField("androidId",   StringType)  ::
      StructField("idfa",        StringType)  ::
      StructField("postBack",    StringType)  ::
      StructField("sendStatus",  IntegerType) ::
      StructField("sendTime",    StringType)  :: //为空，用createTime
      StructField("sv",          StringType)  ::
      StructField("imei",        StringType)  ::
      StructField("imsi",        StringType)  ::
      StructField("imageId",     IntegerType) :: //素材
      StructField("affSub",      StringType)  ::
      StructField("s3",          StringType)  ::
      StructField("s4",          StringType)  ::
      StructField("s5",          StringType)  ::
      StructField("packageName", StringType)  :: //包名
      StructField("domain",      StringType)  :: //域名
//      StructField("reportIp",    StringType)  ::
//      StructField("respStatus",  IntegerType) :: //未下发原因
//      StructField("winPrice",    DoubleType)  :: //
//      StructField("winTime",     StringType)  :: //
      StructField("uid",         StringType)  ::
      StructField("times",       IntegerType) ::
      StructField("time",        IntegerType) ::
      StructField("isNew",       IntegerType) ::
      Nil)
}