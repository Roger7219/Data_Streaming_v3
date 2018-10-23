package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._

class AggUserActiveDWRSchema {
  def structType = AggUserActiveDWRSchema.structType
}

object AggUserActiveDWRSchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("jarId", IntegerType) ::
      StructField("appId", IntegerType) ::
      StructField("countryId", IntegerType) ::
      StructField("carrierId", IntegerType) ::
      StructField("connectType", IntegerType) ::
      StructField("publisherId", IntegerType) ::
      StructField("affSub", StringType) ::

      StructField("activeCount", LongType) ::

      StructField("dwrBusinessDate", StringType) ::
      StructField("dwrLoadTime", StringType) ::
      Nil)
}

