package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, JSONSerializable}
import org.apache.hadoop.hbase.util.Bytes

import scala.beans.BeanProperty
import scala.collection.Map

/**
  * Created by Administrator on 2017/6/8.
  */
//例子
// b_time=2020-09-09 09:00:00
// HivePartitionPart("b_time", "2020-09-09 09:00:00")

// b_time=2020-09-09 09:00:00/l_time=2020-09-09 10:00:00/b_date=2020-09-09
// b_time=2020-09-09 10:00:00/l_time=2020-09-09 10:00:00/b_date=2020-09-09
// Array(
//    Array(HivePartitionPart("b_time", "2020-09-09 09:00:00"), HivePartitionPart("l_time", "2020-09-09 10:00:00"), HivePartitionPart("b_date", "2020-09-09")),
//    Array(HivePartitionPart("b_time", "2020-09-09 10:00:00"), HivePartitionPart("l_time", "2020-09-09 10:00:00"), HivePartitionPart("b_date", "2020-09-09"))
// )
case class HivePartitionPart (@BeanProperty var name: String,
                              @BeanProperty var value: String
                    ) extends JSONSerializable {
  def this(){
    this(null, null)
  }
}
