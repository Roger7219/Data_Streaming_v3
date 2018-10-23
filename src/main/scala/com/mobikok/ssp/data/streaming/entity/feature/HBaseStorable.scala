package com.mobikok.ssp.data.streaming.entity.feature

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.beans.{BeanInfo, BeanProperty}
import scala.collection.Map
/**
  * Rorkey type only is: String/Int/Long
  * Created by Administrator on 2017/6/8.
  */
trait HBaseStorable extends JSONSerializable{
  def toSparkRow (): Row
//
//  /**
//    * 其实是查询和保存时setRowkey<br>
//    * Example:<br>
//    * RowkeyUuid("id_val","name_val").restoreKey(Bytes.toBytes("id_val"))
//    * @param key
//    * @return
//    */
//  def setRowkey (key: Array[Byte]) : HBaseStorable

  /**
    * 其实是保存时getRowkey
    * @return
    */
  def toHBaseRowkey: Array[Byte]


  def toColumns: Map[(String,String), Array[Byte]]

  def assembleFields (row: Array[Byte], source: Map[(String,String), Array[Byte]]) : Unit

  def structType: StructType
}
