package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types._

class NadxTrafficDWISchema{
  def structType = NadxTrafficDWISchema.structType
}

object NadxTrafficDWISchema{

  //注意字段名两边不要含空格！！
  val structType = StructType(
    StructField("dataType",           IntegerType) ::

    StructField("timestamp",          LongType) ::

    StructField("supply_bd_id",       IntegerType) ::
    StructField("supply_am_id",       IntegerType) ::
    StructField("supply_id",          IntegerType) ::
    StructField("supply_protocol",    IntegerType) ::
    StructField("request_flag",       IntegerType) ::

    StructField("ad_format",          IntegerType) ::
    StructField("site_app_id",        IntegerType) ::
    StructField("placement_id",       IntegerType) ::
    StructField("position",           IntegerType) ::

    StructField("country",            StringType) ::
    StructField("state",              StringType) ::
    StructField("city",               StringType) ::

    StructField("carrier",            StringType) ::

    StructField("os",                 StringType) ::
    StructField("os_version",         StringType) ::

    StructField("device_type",        IntegerType) ::
    StructField("device_brand",       StringType) ::
    StructField("device_model",       StringType) ::

    StructField("age",                StringType) ::
    StructField("gender",             StringType) ::

    StructField("cost_currency",      StringType) ::

    // demand
    StructField("demand_bd_id",       IntegerType) ::
    StructField("demand_am_id",       IntegerType) ::
    StructField("demand_id",          IntegerType) ::

    // destination
    StructField("demand_seat_id",     StringType) ::
    StructField("demand_campaign_id", StringType) ::
    StructField("demand_protocol",    IntegerType) ::
    StructField("target_site_app_id", StringType) ::
    StructField("revenue_currency",   StringType) ::

    // common
    StructField("bid_price_model",    IntegerType) ::
    StructField("traffic_type",       IntegerType) ::
    StructField("currency",           StringType) ::

    // id
    StructField("supplyBidId",                       StringType) ::
    StructField("bidRequestId",                      StringType) ::

    StructField("bundle",                            StringType) ::
    StructField("size",                              StringType) ::

    StructField("supply_request_count",              LongType) ::
    StructField("supply_invalid_request_count",      LongType) ::
    StructField("supply_bid_count",                  LongType) ::
    StructField("supply_bid_price_cost_currency",    DoubleType) ::
    StructField("supply_bid_price",                  DoubleType) ::
    StructField("supply_win_count",                  LongType) ::
    StructField("supply_win_price_cost_currency",    DoubleType) ::
    StructField("supply_win_price",                  DoubleType) ::

    StructField("demand_request_count",              LongType) ::
    StructField("demand_bid_count",                  LongType) ::
    StructField("demand_bid_price_revenue_currency", DoubleType) ::
    StructField("demand_bid_price",                  DoubleType) ::
    StructField("demand_win_count",                  LongType) ::
    StructField("demand_win_price_revenue_currency", DoubleType) ::
    StructField("demand_win_price",                  DoubleType) ::
    StructField("demand_timeout_count",              LongType) ::

    StructField("impression_count",                  LongType) ::
    StructField("impression_cost_currency",          DoubleType) ::
    StructField("impression_cost",                   DoubleType) ::
    StructField("impression_revenue_currency",       DoubleType) ::
    StructField("impression_revenue",                DoubleType) ::
    StructField("click_count",                       LongType) ::
    StructField("click_cost_currency",               DoubleType) ::
    StructField("click_cost",                        DoubleType) ::
    StructField("click_revenue_currency",            DoubleType) ::
    StructField("click_revenue",                     DoubleType) ::
    StructField("conversion_count",                  LongType) ::
    StructField("conversion_price",                  DoubleType) ::
    StructField("saveCount",                         IntegerType) ::
      Nil)
}