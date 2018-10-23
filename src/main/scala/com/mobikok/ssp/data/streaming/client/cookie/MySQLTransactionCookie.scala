package com.mobikok.ssp.data.streaming.client.cookie

import scala.beans.BeanProperty

class MySQLTransactionCookie(_parentId: String,
                             _id: String,
                             _targetTable: String) extends TransactionCookie(_parentId, _id) {

  @BeanProperty var targetTable = _targetTable
//  @BeanProperty var partitions = _partitions
}
