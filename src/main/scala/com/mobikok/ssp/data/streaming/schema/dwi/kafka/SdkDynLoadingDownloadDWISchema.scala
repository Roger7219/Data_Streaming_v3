package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class SdkDynLoadingDownloadDWISchema{
  def structType = SdkDynLoadingDownloadDWISchema.structType
}

object SdkDynLoadingDownloadDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("timestamp",       LongType)    ::
      StructField("imei",          StringType)  ::
      StructField("app",           IntegerType) ::
      StructField("jar",           IntegerType) ::
      StructField("country",       StringType)  ::
      StructField("model",         StringType)  ::
      StructField("brand",         StringType)  ::
      StructField("version",       StringType)  ::

      StructField("downloads",         LongType)::
      StructField("success_downloads", LongType)::
      Nil)
}


