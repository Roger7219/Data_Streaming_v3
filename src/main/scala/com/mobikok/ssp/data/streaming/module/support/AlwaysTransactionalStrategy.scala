package com.mobikok.ssp.data.streaming.module.support

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.ssp.data.streaming.util.Logger

/**
  * Created by Administrator on 2017/10/31.
  */
class AlwaysTransactionalStrategy( val dwiLoadTimeDateFormat: SimpleDateFormat, val dwrLoadTimeDateFormat: SimpleDateFormat) extends TransactionalStrategy{

  private val LOG = new Logger(classOf[AlwaysTransactionalStrategy].getName)
  @volatile private var currDwiLoadTime: String = null
  @volatile private var currDwrLoadTime: String = null

  override def dwiLoadTime():String = {
    currDwiLoadTime
  }
  override def dwrLoadTime():String = {
    currDwrLoadTime
  }

  override def initBatch (): Unit ={
    var d = new Date()
    currDwiLoadTime= dwiLoadTimeDateFormat.format(d)
    currDwrLoadTime = dwrLoadTimeDateFormat.format(d)
    LOG.warn("initBatch, Curr batch need transactional action", true)
  }

  //Always return true
  override def needTransactionalAction ():Boolean={
    true
  }

}
