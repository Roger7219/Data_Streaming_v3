package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/6/12.
  */
class TestDWISchema {
  def structType = TestDWISchema.structType
}

object TestDWISchema {

  //注意字段名两边不要含空格！！
  val structType = StructType(
    //单位秒，非毫秒
    StructField("timestamp",          LongType)    ::
      StructField("id",           IntegerType) ::
      StructField("name",           StringType) ::
      StructField("a",           IntegerType) ::
      StructField("b",           IntegerType) ::
      Nil)
}