package com.mobikok.ssp.data.streaming.schema.dwr.kafka

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/8/4.
  */

class AggUserNewDWRSchema{
  def structType = AggUserNewDWRSchema.structType

}

object AggUserNewDWRSchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("jarId", IntegerType) ::
      StructField("appId", IntegerType) ::
      StructField("countryId", IntegerType) ::
      StructField("carrierId", IntegerType) ::
      StructField("connectType", IntegerType) ::
      StructField("publisherId", IntegerType) ::
      StructField("affSub", StringType) ::

      StructField("newCount", LongType) ::

      StructField("dwrBusinessDate", StringType) ::
      StructField("dwrLoadTime", StringType) ::
      Nil)
}

