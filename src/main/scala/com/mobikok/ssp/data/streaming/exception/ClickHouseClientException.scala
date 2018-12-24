package com.mobikok.ssp.data.streaming.exception

class ClickHouseClientException(msg: String, e: Exception) extends RuntimeException(msg, e) {

  def this(msg: String) {
    this(msg, null)
  }

}
