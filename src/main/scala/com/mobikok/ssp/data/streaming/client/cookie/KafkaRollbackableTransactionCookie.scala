package com.mobikok.ssp.data.streaming.client.cookie

import java.util

import com.mobikok.ssp.data.streaming.entity.{OffsetRange, TopicPartition}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.util.OM

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class KafkaRollbackableTransactionCookie (_parentId: String,
                                          _id: String,
                                          _transactionalTmpTable: String,
                                          _targetTable: String,
                                          _transactionalProgressingBackupTable: String,
                                          _transactionalCompletedBackupTable: String,
                                          _offsets: util.Map[TopicPartition, Long],
                                          _offsetRanges: Array[OffsetRange]
                                          ) extends KafkaTransactionCookie(_parentId, _id) {

  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var targetTable: String = _targetTable
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable
  @BeanProperty var offsets: util.Map[TopicPartition, Long] = _offsets
  @BeanProperty var offsetRanges: Array[OffsetRange] = _offsetRanges
}
