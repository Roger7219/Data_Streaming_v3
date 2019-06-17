package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class NdspPerformanceDWISchema{
  def structType = NadxPerformanceDWISchema.structType
}

object NdspPerformanceDWISchema{
  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("type", StringType) ::
      StructField("bidTime", LongType) :: //unix timestamp in second
      StructField("supplyid", IntegerType) ::
      StructField("bidid", StringType) ::
      StructField("impid", StringType) ::
      StructField("price", DoubleType) ::
      StructField("cur", StringType) ::
      StructField("withPrice", BooleanType) ::
      StructField("eventType", IntegerType) ::
      Nil
  )

}

