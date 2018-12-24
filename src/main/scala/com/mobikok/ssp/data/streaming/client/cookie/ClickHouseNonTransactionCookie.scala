package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart

class ClickHouseNonTransactionCookie(_parentId: String,
                                     _id: String,
                                     _targetTable: String,
                                     _hiveLikePartitions : Array[Array[HivePartitionPart]] //Array[Seq[(String, String)]],
                                    ) extends ClickHouseTransactionCookie(_parentId, _id, _targetTable, _hiveLikePartitions) {


}
