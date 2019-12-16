package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class HiveXTrafficDWISchema{
  def structType = HiveXTrafficDWISchema.structType
}


object HiveXTrafficDWISchema{

  //注意字段名两边不要含空格！！

  //注意字段名两边不要含空格！！
  val structType = StructType(
      StructField("timestamp",                         LongType)::
      StructField("app_key",                           StringType)::
      StructField("order_key",                         StringType)::
      StructField("adunit_key",                        StringType)::
      StructField("lineitem_key",                      StringType)::
      StructField("appid",                             StringType)::
      StructField("cid",                               StringType)::
      StructField("city",                              StringType)::
      StructField("ckv",                               StringType)::
      StructField("country_code",                      StringType)::
      StructField("cppck",                             StringType)::
      StructField("current_consent_status",            StringType)::
      StructField("dev",                               StringType)::
      StructField("exclude_adgroups",                  StringType)::
      StructField("gdpr_applies",                      StringType)::
      StructField("id",                                StringType)::
      StructField("is_mraid",                          StringType)::
      StructField("os",                                StringType)::
      StructField("osv",                               StringType)::
      StructField("priority",                          StringType)::
      StructField("req",                               StringType)::
      StructField("reqt",                              StringType)::
      StructField("rev",                               StringType)::
      StructField("udid",                              StringType)::
      StructField("video_type",                        StringType)::
      StructField("request_count",                     LongType)::
      StructField("imp_count",                         LongType)::
      StructField("aclk_count",                        LongType)::
      StructField("attempt_count",                     LongType)::
      Nil)
}

