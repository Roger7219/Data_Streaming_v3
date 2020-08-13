package com.mobikok.ssp.data.streaming.handler.dwr.core

import com.mobikok.ssp.data.streaming.handler.dwr.Handler
import com.mobikok.ssp.data.streaming.module.support.repeats.RepeatsFilter
import org.apache.spark.sql.DataFrame

// 内置
// 过滤dwi中重复的数据
class NonRepeatedPrepareDwrHandler extends Handler {

  var repeatsFilter: RepeatsFilter = _
  def this(repeatsFilter: RepeatsFilter) {
    this()
    this.repeatsFilter = repeatsFilter
    // 无论同步或异步handler都要执行预处理
    isAsynchronous = false
  }

  override def doHandle(unionedPersistenceDwr: DataFrame): DataFrame = {
    unionedPersistenceDwr
  }

  override def doPrepare(dwi: DataFrame): DataFrame = {
    dwi.where(repeatsFilter.dwrNonRepeatedWhere())
  }

  protected def doCommit(): Unit={}
  protected def doClean(): Unit={}
}
