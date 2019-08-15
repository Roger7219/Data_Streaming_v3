package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class NadxScraperDWISchema{
  def structType = NadxScraperDWISchema.structType
}


object NadxScraperDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(

    //{"bundle":"test_bundle1", "timestamp":1564072052}
      StructField("timestamp",          LongType)   ::
      StructField("bundle",             StringType) ::

      Nil)
}
