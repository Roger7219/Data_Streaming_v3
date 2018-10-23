package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._

object AggFeeDWRSchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("jarId", IntegerType) ::
      StructField("appId", IntegerType) ::
      StructField("countryId", IntegerType) ::
      StructField("carrierId", IntegerType) ::
      StructField("connectType", IntegerType) ::
      StructField("publisherId", IntegerType) ::
      StructField("affSub", StringType) ::

      StructField("cost", DoubleType) ::

      StructField("dwrBusinessDate", StringType) ::
      StructField("dwrLoadTime", StringType) ::
      Nil)
}

/**
  * Created by Administrator on 2017/8/4.
  */

class AggFeeDWRSchema{
  def structType = AggFeeDWRSchema.structType
}
