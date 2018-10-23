package com.mobikok.ssp.data.streaming.exception

/**
  * Created by Administrator on 2017/6/15.
  */
class HandlerException (msg: String, e: Exception) extends RuntimeException(msg, e) {

  def this (msg: String) {
    this(msg, null)
  }
}
