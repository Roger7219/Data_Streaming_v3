package com.mobikok.ssp.data.streaming.schema.dm

import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017/6/23.
  */
@deprecated
class SspUserDMSchema {
  def structType = SspUserDMSchema.structType
}
@deprecated
object SspUserDMSchema{
  val structType = StructType(
    StructField("APPID",        IntegerType)  ::
      StructField("COUNTRYID",  IntegerType)  ::
      StructField("CARRIERID",  IntegerType)  ::
      StructField("SDKVERSION", IntegerType)  ::
      StructField("B_DATE",     StringType)   ::
      StructField("TIMES",      LongType)     ::
      StructField("L_TIME",     StringType)  :: Nil
  )
}
