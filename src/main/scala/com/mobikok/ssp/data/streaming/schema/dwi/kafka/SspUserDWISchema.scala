package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2017/6/12.
  */
class SspUserDWISchema {
  def structType = SspUserDWISchema.structType
}

object SspUserDWISchema{
  val structType = StructType(
  StructField("imei",       StringType)  ::
  StructField("imsi",       StringType)  ::
  StructField("createTime", StringType)  ::
  StructField("activeTime", StringType)  ::
  StructField("appId",      IntegerType) ::
  StructField("model",      StringType)  ::
  StructField("version",    StringType)  ::
  StructField("sdkVersion", IntegerType) ::
  StructField("installType",IntegerType) ::
  StructField("leftSize",   StringType)  ::
  StructField("androidId",  StringType)  ::
  StructField("userAgent",  StringType)  ::
  StructField("ipAddr",     StringType)  ::
  StructField("screen",     StringType)  ::
  StructField("countryId",  IntegerType) ::
  StructField("carrierId",  IntegerType) ::
  StructField("sv",         StringType)  ::
  StructField("affSub",     StringType)  ::
  StructField("lat",        StringType)  ::
  StructField("lon",        StringType)  ::
  StructField("mac1",       StringType)  ::
  StructField("mac2",       StringType)  ::
  StructField("ssid",       StringType)  ::
  StructField("lac",        IntegerType) ::
  StructField("cellid",     IntegerType) ::
  StructField("ctype",      IntegerType) ::
    StructField("raterType",     IntegerType)  :: //
    StructField("raterId",      StringType)    :: //
  Nil
  )
}

