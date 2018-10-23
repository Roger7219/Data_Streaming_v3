package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types.{StructType, _}


class SspLogDWISchema{
  def structType = SspLogDWISchema.structType
}

//eg: {"appId":1598,"createTime":"2017-08-02 10:17:22","event":"requestAd","imei":"355075081197611","imsi":"234159183166624","info":"","type":1}
object SspLogDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("appId",          IntegerType) ::
      StructField("createTime",   StringType) ::
      StructField("event",        StringType) ::
      StructField("imei",         StringType) ::
      StructField("imsi",         StringType) ::
      StructField("info",         StringType) ::
      StructField("type",         IntegerType) ::
      Nil)
}