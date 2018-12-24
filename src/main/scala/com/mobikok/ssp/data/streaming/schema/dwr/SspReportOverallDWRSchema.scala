package com.mobikok.ssp.data.streaming.schema.dwr

import org.apache.spark.sql.types._

class SspReportOverallDWRSchema {
  def structType = SspReportOverallDWRSchema.structType
}

object SspReportOverallDWRSchema {
  val structType = StructType(
    StructField("publisherid", IntegerType) ::
      StructField("appid", IntegerType) ::
      StructField("countryid", IntegerType) ::
      StructField("carrierid", IntegerType) ::
      StructField("versionname", StringType) ::
      StructField("adtype", IntegerType) ::
      StructField("campaignid", IntegerType) ::
      StructField("offerid", IntegerType) ::
      StructField("imageid", IntegerType) ::
      StructField("affsub", StringType) ::
      StructField("requestcount", LongType) ::
      StructField("sendcount", LongType) ::
      StructField("showcount", LongType) ::
      StructField("clickcount", LongType) ::
      StructField("feereportcount", LongType) ::
      StructField("feesendcount", LongType) ::
      StructField("feereportprice", DecimalType(19, 10)) ::
      StructField("feesendprice", DecimalType(19, 10)) ::
      StructField("cpcbidprice", DecimalType(19, 10)) ::
      StructField("cpmbidprice", DecimalType(19, 10)) ::
      StructField("conversion", LongType) ::
      StructField("allconversion", LongType) ::
      StructField("revenue", DecimalType(19, 10)) ::
      StructField("realrevenue", DecimalType(19, 10)) ::
      StructField("feecpctimes", LongType) ::
      StructField("feecpmtimes", LongType) ::
      StructField("feecpatimes", LongType) ::
      StructField("feecpasendtimes", LongType) ::
      StructField("feecpcreportprice", DecimalType(19, 10)) ::
      StructField("feecpmreportprice", DecimalType(19, 10)) ::
      StructField("feecpareportprice", DecimalType(19, 10)) ::
      StructField("feecpcsendprice", DecimalType(19, 10)) ::
      StructField("feecpmsendprice", DecimalType(19, 10)) ::
      StructField("feecpasendprice", DecimalType(19, 10)) ::
      StructField("packagename", StringType) ::
      StructField("domain", StringType) ::
      StructField("operatingsystem", StringType) ::
      StructField("systemlanguage", StringType) ::
      StructField("devicebrand", StringType) ::
      StructField("devicetype", StringType) ::
      StructField("browserkernel", StringType) ::
      StructField("respstatus", IntegerType) ::
      StructField("winprice", DecimalType(19, 10)) ::
      StructField("winnotices", LongType) ::
      StructField("test", IntegerType) ::
      StructField("ruleid", IntegerType) ::
      StructField("smartid", IntegerType) ::
      StructField("newcount", LongType) ::
      StructField("activecount", LongType) ::
      StructField("eventname", StringType) ::
      StructField("recommender", IntegerType) ::
      StructField("l_time", StringType) ::
      StructField("b_date", StringType) ::
      StructField("b_time", StringType) ::
      Nil)
}