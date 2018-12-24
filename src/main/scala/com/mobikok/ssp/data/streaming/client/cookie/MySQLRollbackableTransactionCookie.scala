package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart

import scala.beans.BeanProperty

@Deprecated
class MySQLRollbackableTransactionCookie(_parentId: String,
                                         _id: String,
                                         _transactionalTmpTable: String,
                                         _targetTable: String,
                                         _partitions : Array[Array[HivePartitionPart]], //Array[Seq[(String, String)]],
                                         _transactionalProgressingBackupTable: String,
                                         _transactionalCompletedBackupTable: String,
                                         _isEmptyData: Boolean) extends MySQLTransactionCookie(_parentId, _id, _targetTable) {

  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var partitions = _partitions
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable
  @BeanProperty var isEmptyData: Boolean = _isEmptyData
}
