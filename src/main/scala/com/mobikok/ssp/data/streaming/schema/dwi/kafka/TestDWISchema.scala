package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/6/12.
  */
class TestDWISchema {
  def structType = TestDWISchema.structType
}

object TestDWISchema {

  val structType = StructType(
        StructField("appId",       IntegerType) :: //ssp平台申请的id
//        StructField("b_date",      StringType)   ::
//        StructField("l_time",      StringType)   ::
        Nil
  )
}