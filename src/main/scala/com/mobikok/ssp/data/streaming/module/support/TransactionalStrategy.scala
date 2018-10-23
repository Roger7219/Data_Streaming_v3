package com.mobikok.ssp.data.streaming.module.support

import java.text.SimpleDateFormat

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart

/**
  * Created by Administrator on 2017/11/21.
  */
trait TransactionalStrategy {
  def dwiLoadTime(): String
  def dwrLoadTime(): String
  def dwiLoadTimeDateFormat: SimpleDateFormat
  def dwrLoadTimeDateFormat: SimpleDateFormat

  def initBatch (/*dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]]*/): Unit
  def needTransactionalAction ():Boolean
}