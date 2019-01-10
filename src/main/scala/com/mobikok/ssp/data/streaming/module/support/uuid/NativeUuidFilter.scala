package com.mobikok.ssp.data.streaming.module.support.uuid
import org.apache.spark.sql.DataFrame

/**
  * 通过原生的List的方式判断数据有没有重复
  */
class NativeUuidFilter extends UuidFilter {

  override def filter(dwi: DataFrame): DataFrame = {

  }

  override def dwrNonRepeatedWhere(): String = {

  }
}
