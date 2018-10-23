package com.mobikok.ssp.data.streaming.module.support

import java.text.SimpleDateFormat
import java.util.Date
import com.mobikok.ssp.data.streaming.util.Logger


class OptimizedTransactionalStrategy(val dwiLoadTimeDateFormat: SimpleDateFormat, val dwrLoadTimeDateFormat: SimpleDateFormat) extends TransactionalStrategy{
  @volatile private var currDwiLoadTime: String = null
  @volatile private var currDwrLoadTime: String = null

  private val LOG = new Logger(classOf[OptimizedTransactionalStrategy].getName)
//  private var lastTransactionalPersistenceTime = new Date().getTime
//
//  private val EMPTY_PS = Array[Array[HivePartitionPart]]()
//
//  private var lastDwiHivePartitionParts = EMPTY_PS
//  private var lastDwrHivePartitionParts = EMPTY_PS

  @volatile private var isCurrNeedTransactionalAction:Boolean = true

  override def dwiLoadTime():String = {
    currDwiLoadTime
  }
  override def dwrLoadTime():String = {
    currDwrLoadTime
  }

  override def initBatch (/*dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]] */): Unit ={

    var d = new Date()

    var i = dwiLoadTimeDateFormat.format(d)
    var r = dwrLoadTimeDateFormat.format(d)

    if(i.equals(currDwiLoadTime) && r.equals(currDwrLoadTime)) {
      isCurrNeedTransactionalAction = false
    }else {
      currDwiLoadTime = i
      currDwrLoadTime = r
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

  override def needTransactionalAction():Boolean={
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