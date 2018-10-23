package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._

class SspFeeDWRSchema{
  def structType = SspFeeDWRSchema.structType
}
/**
  * Created by Administrator on 2017/8/4.
  */
object SspFeeDWRSchema {

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("publisherId", IntegerType) ::
      StructField("subId",  IntegerType)  ::
      StructField("countryId", IntegerType)  ::
      StructField("carrierId",  IntegerType/*DecimalType(19,10)*/)   :: //carrierId: 运营商
      StructField("sv",  StringType/*DecimalType(19,10)*/)   ::
      StructField("adType",  IntegerType/*DecimalType(19,10)*/)   ::
      StructField("campaignId",  IntegerType/*DecimalType(19,10)*/)   ::
      StructField("offerId",  IntegerType/*DecimalType(19,10)*/)   ::
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

      StructField("times",  LongType/*DecimalType(19,10)*/)   ::
      StructField("sendTimes",  LongType/*DecimalType(19,10)*/)   ::
      StructField("reportPrice",  DecimalType(19,10)/*DecimalType(19,10)*/)   ::
      StructField("sendPrice",  DecimalType(19,10)/*DecimalType(19,10)*/)   ::

      StructField("feeCpcTimes",  LongType)   ::
      StructField("feeCpcReportPrice",  DecimalType(19,10))   ::
      StructField("feeCpcSendPrice",  DecimalType(19,10))   ::
      StructField("feeCpmTimes",  LongType)   ::
      StructField("feeCpmReportPrice",  DecimalType(19,10))   ::
      StructField("feeCpmSendPrice",  DecimalType(19,10))   ::
      StructField("feeCpaTimes",  LongType)   ::
      StructField("feeCpaSendTimes",  LongType)   ::
      StructField("feeCpaReportPrice", DecimalType(19,10))   ::
      StructField("feeCpaSendPrice",  DecimalType(19,10))   ::

      StructField("dwrBusinessDate", StringType)  ::
      StructField("dwrLoadTime",     StringType)  ::
      Nil)
}
