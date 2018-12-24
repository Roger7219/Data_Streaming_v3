package com.mobikok.ssp.data.streaming.handler.dwr

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.module.support.uuid.UuidFilter
import org.apache.spark.sql.DataFrame

//内置
class UUIDFilterDwrHandler extends BeforeFilterHandler {

  var uuidFilter: UuidFilter = _
  def this(uuidFilter: UuidFilter) {
    this()
    this.uuidFilter = uuidFilter
  }

  override def handle(persistenceDwr: DataFrame): (String, DataFrame, TransactionCookie) = {
    ("", persistenceDwr, null)
  }

  override def prepare(dwi: DataFrame): DataFrame = {
    dwi.where(uuidFilter.dwrNonRepeatedWhere())
  }
}
