package com.mobikok.ssp.data.streaming.transaction

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.ssp.data.streaming.util.Logger

/**
  * 策略接口的实现类，输出当前批次的运行的l_time的值，
  * needRealTransactionalAction()方法总是返回true，表示每个批次都需要执行真的事务操作
  */
class AlwaysTransactionalStrategy(val dwiLTimeDateFormat: SimpleDateFormat, val dwrLTimeDateFormat: SimpleDateFormat, dwrShareTable: String) extends TransactionalStrategy{

  private val LOG = new Logger(s"${getClass.getSimpleName}($dwrShareTable)", classOf[AlwaysTransactionalStrategy])
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
