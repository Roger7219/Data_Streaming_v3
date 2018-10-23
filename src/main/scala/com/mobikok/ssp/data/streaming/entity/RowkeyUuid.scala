package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.Map

object RowkeyUuidSchema {

  val structType = StructType(
    StructField("rowkey", StringType) ::
      StructField("uuid", IntegerType) :: Nil
  )
}

/**
  * Create HBase table DDL:<br>
  * create 'user_id', 'f'
  */
case class RowkeyUuid (@BeanProperty var key: String,
                       @BeanProperty var uuid: Int
                    ) extends HBaseStorable {

  val structTypeVal = RowkeyUuidSchema.structType

  def this () {
    this(null, null.asInstanceOf[Int])
  }

//  override def getRowkey: Array[Byte] = {
//    Bytes.toBytes(key)
//  }


//  override def setRowkey (key: Array[Byte]) : RowkeyUuid = {
//    this.key = Bytes.toString(key)
//    this
//  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
//      ("f","rowKey") -> Bytes.toBytes(rowKey),
      ("f","uuid") -> Bytes.toBytes(uuid)
    )
  }

  override def assembleFields (row: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
//    rowkey = Bytes.toString(source.get(("f", "rowkey")).get)
    uuid = Bytes.toInt(source.get(("f", "uuid")).get)
  }

  override def structType: StructType = {
    structTypeVal
  }

  override def toSparkRow (): Row = {
    Row(key, uuid)
  }

  /**
    * 其实是保存时getRowkey
    *
    * @return
    */
  override def toHBaseRowkey: Array[Byte] = {
    Bytes.toBytes(key)
  }
}
