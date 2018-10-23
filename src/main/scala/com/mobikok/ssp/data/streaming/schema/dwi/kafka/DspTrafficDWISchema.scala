package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types.{StructField, _}

class DspTrafficDWISchema {
  def structType = DspTrafficDWISchema.structType
}

object DspTrafficDWISchema {

  val structType = StructType(
      StructField("timestamp", LongType) ::
      StructField("event_type", StringType) ::
      StructField("event_key", StringType) ::
      StructField("request", StructType(
          StructField("click_id", StringType) ::
          StructField("request", StructType(
              StructField("publisher_id", LongType) ::
              StructField("app_id", LongType) ::
              StructField("country_id", LongType) ::
              StructField("carrier_id", LongType) ::
              StructField("ip", StringType) ::
              StructField("raw_request", StringType) :: Nil
          )) ::
         StructField("trade", StructType(
              StructField("deal_type", StringType) ::
              StructField("dsp_id", LongType) ::
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
              StructField("publisher_id", LongType) ::
              StructField("app_id", LongType) ::
              StructField("country_id", LongType) ::
              StructField("carrier_id", LongType) ::
              StructField("ip", StringType) ::
              StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
              StructField("deal_type", StringType) ::
              StructField("dsp_id", LongType) ::
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
              StructField("publisher_id", LongType) ::
              StructField("app_id", LongType) ::
              StructField("country_id", LongType) ::
              StructField("carrier_id", LongType) ::
              StructField("ip", StringType) ::
              StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
              StructField("deal_type", StringType) ::
              StructField("dsp_id", LongType) ::
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
              StructField("publisher_id", LongType) ::
              StructField("app_id", LongType) ::
              StructField("country_id", LongType) ::
              StructField("carrier_id", LongType) ::
              StructField("ip", StringType) ::
              StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
              StructField("deal_type", StringType) ::
              StructField("dsp_id", LongType) ::
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
              StructField("publisher_id", LongType) ::
              StructField("app_id", LongType) ::
              StructField("country_id", LongType) ::
              StructField("carrier_id", LongType) ::
              StructField("ip", StringType) ::
              StructField("raw_request", StringType) :: Nil
          )) ::
          StructField("trade", StructType(
              StructField("deal_type", StringType) ::
              StructField("dsp_id", LongType) ::
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