package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class PhoenixTransactionCookie (_parentId:String,
                                _id: String,
                                _transactionalTmpTable: String,
                                _targetTable: String,
                                _transactionalProgressingBackupTable: String,
                                _transactionalCompletedBackupTable: String
                                ) extends TransactionCookie(_parentId, _id){

  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var targetTable: String = _targetTable
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable

}
