package com.mobikok.ssp.data.streaming.config

/**
  * Created by Administrator on 2018/2/6.
  */
object DynamicConfig {

  val BATCH_PROCESSING_TIMEOUT_MS = "batch.processing.timeout.ms"

  def of(appName: String, configName: String) :String={
    s"$appName.$configName"
  }
}
