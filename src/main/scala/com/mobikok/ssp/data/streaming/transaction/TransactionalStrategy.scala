package com.mobikok.ssp.data.streaming.transaction

import java.text.SimpleDateFormat

/**
  * 策略接口，输出当前批次的运行的l_time的值，
  * 以及关键方法needRealTransactionalAction()，判断当前批次是否需要执行真的事务操作
  *
  * Created by Administrator on 2017/11/21.
  */
trait TransactionalStrategy {
  def dwiLTime(): String
  def dwrTime(): String
  def dwiLTimeDateFormat: SimpleDateFormat
  def dwrLTimeDateFormat: SimpleDateFormat

  def initBatch (/*dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]]*/): Unit
  def needRealTransactionalAction():Boolean
}