package com.mobikok.ssp.data.streaming.handler.dwr.core

import com.mobikok.ssp.data.streaming.handler.dwr.BeforeFilterHandler
import com.mobikok.ssp.data.streaming.module.support.uuid.UuidFilter
import org.apache.spark.sql.DataFrame

//内置
class UUIDFilterDwrHandler extends BeforeFilterHandler {

  var uuidFilter: UuidFilter = _
  def this(uuidFilter: UuidFilter) {
    this()
    this.uuidFilter = uuidFilter
    // 预处理可以是同步或异步
    isAsynchronous = false
  }

  override def handle(unionedPersistenceDwr: DataFrame): DataFrame = {
    unionedPersistenceDwr
  }

  override def prepare(dwi: DataFrame): DataFrame = {
    dwi.where(uuidFilter.dwrNonRepeatedWhere())
  }
}
