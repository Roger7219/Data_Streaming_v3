package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, JSONSerializable}
import org.apache.hadoop.hbase.util.Bytes

import scala.beans.BeanProperty
import scala.collection.Map

/**
  * Created by Administrator on 2017/6/8.
  */
case class HivePartitionPart (@BeanProperty var name: String,
                              @BeanProperty var value: String
                    ) extends JSONSerializable {
  def this(){
    this(null, null)
  }
}
