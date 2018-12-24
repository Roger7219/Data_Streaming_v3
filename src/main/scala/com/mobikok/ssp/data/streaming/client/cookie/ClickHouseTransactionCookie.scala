package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart

import scala.beans.BeanProperty

class ClickHouseTransactionCookie (_parentId: String,
                                   _id: String,
                                   _targetTable: String,
                                   _hiveLikePartitions : Array[Array[HivePartitionPart]]
                                  ) extends TransactionCookie(_parentId, _id) {

  @BeanProperty var targetTable: String = _targetTable
  @BeanProperty var partitions: Array[Array[HivePartitionPart]] = _hiveLikePartitions

}
