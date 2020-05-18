package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class NadxGEODWISchema{
  def structType = NadxGEODWISchema.structType
}

object NadxGEODWISchema{

  //demand_id,crid, os,country,adm,demand_crid_count

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("timestamp", LongType) ::
    StructField("demand_id", IntegerType) ::
    StructField("crid",      StringType)  ::
    StructField("os",        StringType)  ::
    StructField("country",   StringType)  ::
    StructField("adm",       StringType)  ::
      Nil)
}