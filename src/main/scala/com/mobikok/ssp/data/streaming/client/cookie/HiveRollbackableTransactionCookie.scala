package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import org.apache.spark.sql.SaveMode

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class HiveRollbackableTransactionCookie (_parentId: String,
                                         _id: String,
                                         _transactionalTmpTable: String,
                                         _targetTable: String,
                                         _saveMode: SaveMode,
                                         _partitions : Array[Array[HivePartitionPart]], //Array[Seq[(String, String)]],
                                         _transactionalProgressingBackupTable: String,
                                         _transactionalCompletedBackupTable: String,
                                         _isEmptyData: Boolean
                                        ) extends HiveTransactionCookie(_parentId, _id, _targetTable, _partitions){


  @BeanProperty var transactionalTmpTable: String = _transactionalTmpTable
  @BeanProperty var saveMode: SaveMode = _saveMode
  @BeanProperty var transactionalProgressingBackupTable: String = _transactionalProgressingBackupTable
  @BeanProperty var transactionalCompletedBackupTable: String = _transactionalCompletedBackupTable
  @BeanProperty var isEmptyData: Boolean = _isEmptyData
}
