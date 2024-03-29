package com.mobikok.ssp.data.streaming.transaction

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.ssp.data.streaming.util.Logger


/**
  * 策略接口的实现类，输出当前批次的运行的l_time的值，
  * needRealTransactionalAction()在每个小时的首次批次返回true，即每隔一小时执行一次真的事务操作
  */
class OptimizedTransactionalStrategy(val dwiLTimeDateFormat: SimpleDateFormat, val dwrLTimeDateFormat: SimpleDateFormat, dwrShareTable: String) extends TransactionalStrategy{
  @volatile private var currDwiLTime: String = null
  @volatile private var currDwrLTime: String = null

  private val LOG = new Logger(s"${getClass.getSimpleName}($dwrShareTable)", classOf[OptimizedTransactionalStrategy])
//  private var lastTransactionalPersistenceTime = new Date().getTime
//
//  private val EMPTY_PS = Array[Array[HivePartitionPart]]()
//
//  private var lastDwiHivePartitionParts = EMPTY_PS
//  private var lastDwrHivePartitionParts = EMPTY_PS

  @volatile private var isCurrNeedTransactionalAction:Boolean = true

  override def dwiLTime():String = {
    currDwiLTime
  }
  override def dwrTime():String = {
    currDwrLTime
  }

  override def initBatch (/*dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]] */): Unit ={

    var d = new Date()

    var i = dwiLTimeDateFormat.format(d)
    var r = dwrLTimeDateFormat.format(d)

    if(i.equals(currDwiLTime) && r.equals(currDwrLTime)) {
      isCurrNeedTransactionalAction = false
    }else {
      currDwiLTime = i
      currDwrLTime = r
      isCurrNeedTransactionalAction = true
    }
//    var ips = dwiHivePartitionParts
//    var rps = dwrHivePartitionParts
//
//    if(dwiHivePartitionParts == null) {
//      ips = EMPTY_PS
//    }
//    if(dwrHivePartitionParts == null) {
//      rps = EMPTY_PS
//    }
//
//    val currDwiAndDwrHivePartitionPartsJSON = OM.toJOSN(ips ++ rps);
//    val lastDwiAndDwrHivePartitionPartsJSON = OM.toJOSN(lastDwiHivePartitionParts ++ lastDwrHivePartitionParts)
//
//    if(currDwiAndDwrHivePartitionPartsJSON.equals(lastDwiAndDwrHivePartitionPartsJSON)) {
//      isCurrNeedTransactionalAction = false
//    }else {
//      lastDwiHivePartitionParts = ips
//      lastDwrHivePartitionParts = rps
//      isCurrNeedTransactionalAction = true
//    }
//
//    if(!isCurrNeedTransactionalAction && System.currentTimeMillis() - lastTransactionalPersistenceTime >= 1000*60*60) {
//      isCurrNeedTransactionalAction = true
//    }
//
//    if(isCurrNeedTransactionalAction) {
//      lastTransactionalPersistenceTime = System.currentTimeMillis()
//    }

    LOG.warn("initBatch, Curr batch need transactional action", isCurrNeedTransactionalAction)
  }

  override def needRealTransactionalAction():Boolean={
    isCurrNeedTransactionalAction
  }

//  def dwrHivePartitionParts(): Array[Array[HivePartitionPart]] ={
//    lastDwrHivePartitionParts
//  }
//
//
//  def dwiHivePartitionParts(): Array[Array[HivePartitionPart]] ={
//    lastDwiHivePartitionParts
//  }


}