package com.mobikok.ssp.data.streaming.schema.dm

import org.apache.spark.sql.types._

class SspCampaignSchema {
  def structType = SspCampaignSchema.structType
}

object SspCampaignSchema {

  val structType = StructType(
    StructField("appId", IntegerType) ::
    StructField("offerId", IntegerType) ::
    StructField("sendcount", LongType) ::
    StructField("realrevenue", DoubleType) ::
    Nil
  )
}
