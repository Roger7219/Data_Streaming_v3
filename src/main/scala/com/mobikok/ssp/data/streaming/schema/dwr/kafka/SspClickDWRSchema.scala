package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/8/4.
  */

class SspClickDWRSchema{
  def structType = SspClickDWRSchema.structType
}
object SspClickDWRSchema {
  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("publisherId", IntegerType) ::
      StructField("subId", IntegerType) ::
      StructField("countryId", IntegerType) ::
      StructField("carrierId", IntegerType) ::
      StructField("sv", StringType) ::
      StructField("adType", IntegerType) ::
      StructField("campaignId", IntegerType) ::
      StructField("offerId", IntegerType) ::
      StructField("imageId",  IntegerType)   ::
      StructField("affSub",  StringType)   ::

      StructField("packageName", StringType)::
      StructField("domain", StringType)::
      StructField("operatingSystem", StringType)::
      StructField("systemLanguage", StringType)::
      StructField("deviceBrand", StringType)::
      StructField("deviceType", StringType)::
      StructField("browserKernel", StringType)::
      StructField("b_time", StringType)::

      StructField("times", LongType) ::
      StructField("cpcTimes", LongType) ::
      StructField("cpcBidPrice", DecimalType(19, 10)) ::
      StructField("cpcSendPrice", DecimalType(19, 10)) ::

      StructField("dwrBusinessDate", StringType) ::
      StructField("dwrLoadTime", StringType) ::
      Nil)
}