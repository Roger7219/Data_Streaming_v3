package com.mobikok.ssp.data.streaming.exception

class AsyncHandlerWorkerException(msg: String, e: Throwable) extends RuntimeException(msg, e) {

  def this(msg: String) {
    this(msg, null)
  }

}
