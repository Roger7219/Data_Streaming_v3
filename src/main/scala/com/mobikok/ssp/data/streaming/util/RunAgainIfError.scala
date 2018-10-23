package com.mobikok.ssp.data.streaming.util

import org.apache.commons.lang3.exception.ExceptionUtils


/**
  * Created by admin on 2017/11/13.
  */
object RunAgainIfError {
  private val LOG = new Logger(RunAgainIfError.getClass)

  def run[T](func: => T): T = {
    run(func, "Run fail")
  }

  def run[T](func: => T, errorTip: String): T = {
    run(func, errorTip, {x=>})
  }

  def run[T](func: => T, errorCallback: Throwable =>Unit): T = {
    run(func, "", errorCallback)
  }

  def run[T](func: => T, errorTip: String, errorCallback: Throwable =>Unit): T = {
    var returnVal:T = null.asInstanceOf[T]
    var b = true
    while(b) {

      try {
        returnVal = func
        b = false
      }catch {case e:Throwable=>
        if(StringUtil.isEmpty(errorTip)) {
          LOG.warn(s"Will try to run again !!", "Exception", ExceptionUtils.getStackTrace(e))
        }else {
          LOG.warn(s"Will try to run again !!", "Error tip", errorTip, "Exception", ExceptionUtils.getStackTrace(e))
        }

        try {
          if(errorCallback != null) {
            errorCallback(e)
          }
        }catch {
          case t:Throwable=>
            LOG.error("Call errorCallback() fail:", t)
        }
        Thread.sleep(10*1000)
      }
    }
    returnVal
  }

}
//
//object test001{
//  def main (args: Array[String]): Unit = {
//     RunAgainIfError.run({
//      println("wqe")
//    })
//
//  }
//}
