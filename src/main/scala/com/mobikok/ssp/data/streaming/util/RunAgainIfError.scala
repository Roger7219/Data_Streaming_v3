package com.mobikok.ssp.data.streaming.util

import org.apache.commons.lang3.exception.ExceptionUtils


/**
  * Created by admin on 2017/11/13.
  */
object RunAgainIfError {
  private val LOG = new Logger(RunAgainIfError.getClass.getSimpleName, RunAgainIfError.getClass)

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
    run(func, errorTip, errorCallback, Integer.MAX_VALUE)
  }
//
//  def run[T](func: => T, errorTip: String, maxTries: Int): T = {
//    run(func, errorTip, null, maxTries)
//  }

  def runForJava[T](func: Action[T], errorTip: String, maxTries: Int): T = {
    run({func.doAction()}, errorTip, {x=>}, maxTries)
  }

  def run[T](func: => T, errorTip: String, errorCallback: Throwable =>Unit, maxTries: Int): T = {
    var returnVal:T = null.asInstanceOf[T]
    var b = true
    var tries = 0
    var throwable: Throwable = null
    while(b) {

      try {
        tries = tries + 1
        returnVal = func
        b = false
      }catch {case e: Throwable=>
        throwable = e;
        if(StringUtil.isEmpty(errorTip)) {
          LOG.warn(s"Will try to run again after 60s !!", "Exception", ExceptionUtils.getStackTrace(e))
        }else {
          LOG.warn(s"Will try to run again after 60s !!", "Error tip", errorTip, "Exception", ExceptionUtils.getStackTrace(e))
        }

        try {
          if(errorCallback != null) {
            errorCallback(e)
          }
        }catch {
          case t:Throwable=>
            LOG.error("Call errorCallback() fail:", t)
        }
        Thread.sleep(60*1000)
      }

      if (tries >= maxTries)
        throw new RuntimeException("Run again tries " + tries + ", But still failed !!!", throwable)


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
