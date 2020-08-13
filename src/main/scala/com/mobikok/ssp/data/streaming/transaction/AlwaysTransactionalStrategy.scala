package com.mobikok.ssp.data.streaming.transaction

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.ssp.data.streaming.util.Logger

/**
  * Created by Administrator on 2017/10/31.
  */
class AlwaysTransactionalStrategy(val dwiLTimeDateFormat: SimpleDateFormat, val dwrLTimeDateFormat: SimpleDateFormat) extends TransactionalStrategy{

  private val LOG = new Logger(classOf[AlwaysTransactionalStrategy].getName)
  @volatile private var currDwiLoadTime: String = null
  @volatile private var currDwrLoadTime: String = null

  override def dwiLTime():String = {
    currDwiLoadTime
  }
  override def dwrTime():String = {
    currDwrLoadTime
  }

  override def initBatch (): Unit ={
    var d = new Date()
    currDwiLoadTime= dwiLTimeDateFormat.format(d)
    currDwrLoadTime = dwrLTimeDateFormat.format(d)
    LOG.warn("initBatch, Curr batch need transactional action", true)
  }

  //Always return true
  override def needRealTransactionalAction():Boolean={
    true
  }

}
