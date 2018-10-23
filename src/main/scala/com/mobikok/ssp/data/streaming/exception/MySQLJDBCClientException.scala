package com.mobikok.ssp.data.streaming.exception

/**
  * Created by Administrator on 2017/6/16.
  */
class MySQLJDBCClientException (msg: String, e: Exception) extends RuntimeException(msg, e) {

  def this (msg: String) {
    this(msg, null)
  }
}
