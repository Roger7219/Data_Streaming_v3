package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.Map

object UuidSchema {

  val structType = StructType(
    StructField("uuid", StringType)
    :: Nil
  )
}


case class Uuid (@BeanProperty var uuid: String
                    ) extends HBaseStorable {

  def this () {
    this(null)
  }

  override def toHBaseRowkey: Array[Byte] = {
    if(uuid == null ) null else Bytes.toBytes(uuid)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
      ("f","uuid") -> Bytes.toBytes(uuid)
    )
  }

  override def assembleFields (key: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
//    uuid = Bytes.toString(key)
    uuid = Bytes.toString(source.get(("f", "uuid")).get)
  }



  override def structType: StructType = UuidStatSchema.structType

  override def toSparkRow (): Row = {
    Row(uuid)
  }
}
