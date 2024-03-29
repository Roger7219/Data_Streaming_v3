package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import org.apache.spark.sql.SaveMode

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class HiveNonTransactionCookie (_parentId: String,
                                _id: String,
                                _targetTable: String,
                                _saveMode: SaveMode,
                                _partitions : Array[Array[HivePartitionPart]] //Array[Seq[(String, String)]],
                               ) extends HiveTransactionCookie(_parentId, _id, _targetTable, _saveMode, _partitions){

}
