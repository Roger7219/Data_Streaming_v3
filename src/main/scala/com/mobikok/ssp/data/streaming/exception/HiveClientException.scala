package com.mobikok.ssp.data.streaming.exception

import com.sun.tracing.dtrace.ModuleName

/**
  * Created by Administrator on 2017/6/14.
  */
class HiveClientException (msg: String, e: Throwable) extends RuntimeException(msg, e){

  def this(msg: String){
    this(msg, null)
  }

}
