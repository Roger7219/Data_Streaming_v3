package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import org.apache.spark.sql.SaveMode

import scala.beans.BeanProperty

class ClickHouseRollbackableTransactionCookie(_parentId: String,
                                              _id: String,
                                              _transactionalTmpTable: String,
                                              _targetTable: String,
                                              _saveMode: SaveMode,
                                              _hiveLikePartitions : Array[Array[HivePartitionPart]],
                                              _transactionalProgressingBackupTable: String,
                                              _transactionalCompletedBackupTable: String,
                                              _isEmptyData: Boolean
                                             ) extends ClickHouseTransactionCookie(_parentId, _id, _targetTable, _hiveLikePartitions) {

  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var saveMode: SaveMode = _saveMode
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable
  @BeanProperty var isEmptyData: Boolean = _isEmptyData
}
