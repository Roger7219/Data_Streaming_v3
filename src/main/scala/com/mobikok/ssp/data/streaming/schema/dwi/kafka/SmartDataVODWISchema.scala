package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class SmartDataVODWISchema{
  def structType = SmartDataVODWISchema.structType
}

object SmartDataVODWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("id",           IntegerType) ::
      StructField("campaignId",   IntegerType) ::
      StructField("s1",           StringType)  ::
      StructField("s2",           StringType)  ::
      StructField("createTime",   StringType)  ::
      StructField("offerId",      IntegerType) ::
      StructField("countryId",    IntegerType) ::
      StructField("carrierId",    IntegerType) ::
      StructField("deviceType",   IntegerType) ::
      StructField("userAgent",    StringType)  ::
      StructField("ipAddr",       StringType)  ::
      StructField("clickId",      StringType)  ::
      StructField("price",        DoubleType)  ::
      StructField("status",       IntegerType) ::
      StructField("sendStatus",   IntegerType) ::
      StructField("reportTime",   StringType)  ::
      StructField("sendPrice",    DoubleType)  ::
      StructField("frameId",      IntegerType) ::
      StructField("referer",      StringType)  ::
      StructField("isTest",       IntegerType) ::
      StructField("times",        IntegerType) ::
      StructField("res",          StringType)  ::
      StructField("type",         StringType)  ::
      StructField("clickUrl",     StringType)  ::
      StructField("reportIp",     StringType)  ::
      Nil)
}