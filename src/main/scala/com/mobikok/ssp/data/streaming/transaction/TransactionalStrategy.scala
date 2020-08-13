package com.mobikok.ssp.data.streaming.transaction

import java.text.SimpleDateFormat

/**
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