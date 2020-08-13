package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.transaction.TransactionCookie

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class HBaseTransactionCookie (_parentId:String,
                              _id: String,
                              _transactionalTmpTable: String,
                              _targetTable: String,
                              _hbaseStorableImplClass: Class[_ <: HBaseStorable],
                              _transactionalProgressingBackupTable: String,
                              _transactionalCompletedBackupTable: String,
                              _count: Long
                             ) extends TransactionCookie(_parentId, _id){


  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var targetTable: String = _targetTable
  @BeanProperty var hbaseStorableImplClass: Class[_ <: HBaseStorable] = _hbaseStorableImplClass
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable
  @BeanProperty var count: Long = _count

}
