package com.mobikok.ssp.data.streaming.exception

import org.apache.commons.lang3.exception.ExceptionUtils

/**
  * Created by Administrator on 2017/6/8.
  */
class ModuleTracerException(msg: String, e: Throwable) extends RuntimeException(
  if (e == null) msg else msg + "\nCaused by: " + ExceptionUtils.getStackTrace(e),
  e
) {

  def this (msg: String) {
    this (msg, null)
  }
}
