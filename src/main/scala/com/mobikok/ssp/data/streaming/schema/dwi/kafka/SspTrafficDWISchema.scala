package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types.{StructType, _}

class SspTrafficDWISchema{
  def structType = SspTrafficDWISchema.structType
}
object SspTrafficDWISchema{

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
      StructField("respStatus",  IntegerType) :: //未下发原因
      StructField("winPrice",    DoubleType)  :: //中签价格
      StructField("winTime",     StringType)  :: //中签时间
      StructField("appPrice",    DoubleType)  :: //app价格 Dsp V1.0.1 新增
      StructField("test",        IntegerType) :: //Dsp A/B测试 Dsp V1.0.1 新增
      StructField("ruleId",      IntegerType) :: //smartLink + 新增
      StructField("smartId",     IntegerType) ::
      StructField("pricePercent",   IntegerType) :: //单价比例
      StructField("appPercent",     IntegerType) :: //app比例
      StructField("salePercent",    IntegerType) :: //按条比例
      StructField("appSalePercent", IntegerType) ::
      StructField("reportIp",       StringType)  ::
      StructField("eventName",      StringType)  ::
      StructField("eventValue",     IntegerType) ::
      StructField("refer",          StringType)  ::
      StructField("status",         IntegerType) ::
      StructField("city",           StringType)  ::
      StructField("region",         StringType)  ::
      StructField("uid",            StringType)  ::
      StructField("times",          IntegerType) ::
      StructField("time",           IntegerType) ::
      StructField("isNew",          IntegerType) ::
      StructField("pbResp",         StringType)  :: //postback response信息
      StructField("recommender",    IntegerType)  :: //推荐框架的算法标记
        Nil)
}