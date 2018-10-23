package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class AdxTrafficDWISchema {
  def structType = AdxTrafficDWISchema.structType
}



object AdxTrafficDWISchema {

  val structType = StructType(
    StructField("timestamp", LongType) ::
      StructField("event_type", StringType) ::
      StructField("event_key", StringType) ::
      StructField("request", StructType(
          StructField("click_id", StringType) ::
          StructField("request", StructType(
            StructField("publisher_id", IntegerType) ::
            StructField("app_id", IntegerType) ::
            StructField("country_id", IntegerType) ::
            StructField("carrier_id", IntegerType) ::
            StructField("ad_id", IntegerType) ::
            StructField("ad_type", IntegerType) ::
            StructField("width", IntegerType) ::
            StructField("height", IntegerType) ::
            StructField("iab1", StringType) ::
            StructField("iab2", StringType) ::
            StructField("ip", StringType) ::
            StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
            StructField("deal_type", StringType) ::
            StructField("dsp_id", IntegerType) ::
            StructField("status", IntegerType) ::
            StructField("is_show", IntegerType) ::
            StructField("bid_price", DoubleType) ::
            StructField("clear_price", DoubleType) ::
            StructField("media_price", DoubleType) ::
            StructField("win_noticeUrl", StringType) ::
            StructField("click_url", StringType) ::
            StructField("pixel_url", StringType) ::
            StructField("raw_response", StringType) :: Nil
          ))
          :: Nil
      )) ::
      StructField("send", StructType(
        StructField("click_id", StringType) ::
          StructField("request", StructType(
            StructField("publisher_id", IntegerType) ::
            StructField("app_id", IntegerType) ::
            StructField("country_id", IntegerType) ::
            StructField("carrier_id", IntegerType) ::
            StructField("ad_id", IntegerType) ::
            StructField("ad_type", IntegerType) ::
            StructField("width", IntegerType) ::
            StructField("height", IntegerType) ::
            StructField("iab1", StringType) ::
            StructField("iab2", StringType) ::
            StructField("ip", StringType) ::
            StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
            StructField("deal_type", StringType) ::
            StructField("dsp_id", IntegerType) ::
            StructField("status", IntegerType) ::
            StructField("is_show", IntegerType) ::
            StructField("bid_price", DoubleType) ::
            StructField("clear_price", DoubleType) ::
            StructField("media_price", DoubleType) ::
            StructField("win_noticeUrl", StringType) ::
            StructField("click_url", StringType) ::
            StructField("pixel_url", StringType) ::
            StructField("raw_response", StringType) :: Nil
          ))
          :: Nil
      )) ::
      StructField("win_notice", StructType(
        StructField("click_id", StringType) ::
          StructField("request", StructType(
            StructField("publisher_id", IntegerType) ::
            StructField("app_id", IntegerType) ::
            StructField("country_id", IntegerType) ::
            StructField("carrier_id", IntegerType) ::
            StructField("ad_id", IntegerType) ::
            StructField("ad_type", IntegerType) ::
            StructField("width", IntegerType) ::
            StructField("height", IntegerType) ::
            StructField("iab1", StringType) ::
            StructField("iab2", StringType) ::
            StructField("ip", StringType) ::
            StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
            StructField("deal_type", StringType) ::
            StructField("dsp_id", IntegerType) ::
            StructField("status", IntegerType) ::
            StructField("is_show", IntegerType) ::
            StructField("bid_price", DoubleType) ::
            StructField("clear_price", DoubleType) ::
            StructField("media_price", DoubleType) ::
            StructField("win_noticeUrl", StringType) ::
            StructField("click_url", StringType) ::
            StructField("pixel_url", StringType) ::
            StructField("raw_response", StringType) :: Nil
          ))
          :: Nil
      )) ::
      StructField("impression", StructType(
        StructField("click_id", StringType) ::
          StructField("request", StructType(
            StructField("publisher_id", IntegerType) ::
            StructField("app_id", IntegerType) ::
            StructField("country_id", IntegerType) ::
            StructField("carrier_id", IntegerType) ::
            StructField("ad_id", IntegerType) ::
            StructField("ad_type", IntegerType) ::
            StructField("width", IntegerType) ::
            StructField("height", IntegerType) ::
            StructField("iab1", StringType) ::
            StructField("iab2", StringType) ::
            StructField("ip", StringType) ::
            StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
            StructField("deal_type", StringType) ::
            StructField("dsp_id", IntegerType) ::
            StructField("status", IntegerType) ::
            StructField("is_show", IntegerType) ::
            StructField("bid_price", DoubleType) ::
            StructField("clear_price", DoubleType) ::
            StructField("media_price", DoubleType) ::
            StructField("win_noticeUrl", StringType) ::
            StructField("click_url", StringType) ::
            StructField("pixel_url", StringType) ::
            StructField("raw_response", StringType) :: Nil
          ))
          :: Nil
      )) ::
      StructField("click", StructType(
        StructField("click_id", StringType) ::
          StructField("request", StructType(
            StructField("publisher_id", IntegerType) ::
            StructField("app_id", IntegerType) ::
            StructField("country_id", IntegerType) ::
            StructField("carrier_id", IntegerType) ::
            StructField("ad_id", IntegerType) ::
            StructField("ad_type", IntegerType) ::
            StructField("width", IntegerType) ::
            StructField("height", IntegerType) ::
            StructField("iab1", StringType) ::
            StructField("iab2", StringType) ::
            StructField("ip", StringType) ::
            StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
            StructField("deal_type", StringType) ::
            StructField("dsp_id", IntegerType) ::
            StructField("status", IntegerType) ::
            StructField("is_show", IntegerType) ::
            StructField("bid_price", DoubleType) ::
            StructField("clear_price", DoubleType) ::
            StructField("media_price", DoubleType) ::
            StructField("win_noticeUrl", StringType) ::
            StructField("click_url", StringType) ::
            StructField("pixel_url", StringType) ::
            StructField("raw_response", StringType) :: Nil
          ))
          :: Nil
      ))
      :: Nil)
}