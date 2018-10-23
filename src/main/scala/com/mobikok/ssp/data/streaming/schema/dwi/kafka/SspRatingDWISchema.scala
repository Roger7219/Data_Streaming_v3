package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class SspRatingDWISchema{
  def structType = SspRatingDWISchema.structType
}
object SspRatingDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("userId",      IntegerType) ::
      StructField("offerId",     IntegerType) ::
      StructField("imei",        StringType)  ::
      StructField("rating",     IntegerType)  ::
      StructField("dwrBusinessDate", StringType)  ::
      StructField("dwrLoadTime",     StringType)  ::
        Nil
  )
}