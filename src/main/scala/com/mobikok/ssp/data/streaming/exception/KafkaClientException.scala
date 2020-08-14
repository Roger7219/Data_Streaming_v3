package com.mobikok.ssp.data.streaming.exception

/**
  * Created by Administrator on 2017/6/15.
  */
class KafkaClientException (msg: String, e: Throwable) extends RuntimeException(msg, e) {

  def this (msg: String) {
    this(msg, null)
  }
}
