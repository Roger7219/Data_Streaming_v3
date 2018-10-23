package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._


/**
  * Created by Administrator on 2017/8/4.
  */
class SspUserNewDWRSchema{
  def structType = SspUserNewDWRSchema.structType
}

object SspUserNewDWRSchema {

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("appId",  IntegerType)  ::
      StructField("countryId", IntegerType)  ::
      StructField("carrierId",  IntegerType)   :: //carrierId: 运营商
      StructField("sv",  StringType)   ::
      StructField("affSub",  StringType)   ::

      StructField("operatingSystem", StringType)::
      StructField("systemLanguage", StringType)::
      StructField("deviceBrand", StringType)::
      StructField("deviceType", StringType)::
      StructField("browserKernel", StringType)::
      StructField("b_time", StringType)::

      StructField("newCount",  LongType/*DecimalType(19,10)*/)   ::

      StructField("dwrBusinessDate", StringType)  ::
      StructField("dwrLoadTime",     StringType)  ::
      Nil)
}