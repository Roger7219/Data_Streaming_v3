package com.mobikok.ssp.data.streaming.schema.dwi.kafka

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 用于离线调度的，为了沿用流统计配置，从kafka消费空数据，实际上用不上kafka，只是用流统计这个调度机制
  */
class EmptyDWISchema{
  def structType = EmptyDWISchema.structType
}
object EmptyDWISchema{

  // 字段定义随意，保持这种格式，做个样子
  val structType = StructType(
    StructField("empty_time",         StringType) :: Nil)
}