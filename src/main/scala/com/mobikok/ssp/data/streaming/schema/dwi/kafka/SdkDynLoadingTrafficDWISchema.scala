package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class SdkDynLoadingTrafficDWISchema{
  def structType = SdkDynLoadingTrafficDWISchema.structType
}

object SdkDynLoadingTrafficDWISchema{

//   、Adv BD、Advertiser、JAR、Pub BD、Publisher、App、Country、Brand、Verison、New User、Active User、Request、Response、Load、Load Success、Revenue、Cost

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("timestamp",     LongType)      ::
      StructField("adv",           IntegerType)   ::
      StructField("adv_db",        IntegerType)   ::
      StructField("jar",           IntegerType)   ::
      StructField("pub",           IntegerType)   ::
      StructField("pub_bd",        IntegerType)   ::
      StructField("app",           IntegerType)   ::
      StructField("country",       StringType)    ::
      StructField("model",          StringType)   ::
      StructField("brand",         StringType)    ::
      StructField("version",       StringType)    ::

      StructField("requests",      LongType)      ::
      StructField("responses",     LongType)      ::
      StructField("loads",         LongType)      ::
      StructField("success_loads", LongType)      ::
      StructField("revenue",       DoubleType)    ::
      StructField("cost",          DoubleType)    ::
        Nil)
}
