package com.mobikok.ssp.data.streaming.entity

import java.util.UUID

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.schema.dm.UuidStatSchema
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.Map

object UuidStatSchema {

  val structType = StructType(
    StructField("uuid", StringType) ::
      StructField("repeats", IntegerType) :: Nil
  )
}

/**
  * Created by Administrator on 2017/6/8.
  */
case class UuidStat (@BeanProperty var uuid: String,
                     @BeanProperty var repeats: Int
                    ) extends HBaseStorable {

  def this () {
    this(null, null.asInstanceOf[Int])
  }

  override def toHBaseRowkey: Array[Byte] = {
    if(uuid == null ) Bytes.toBytes("null_" + UUID.randomUUID().toString) else Bytes.toBytes(uuid)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
      ("f","uuid") -> Bytes.toBytes(uuid),
      ("f","repeats") -> Bytes.toBytes(repeats)
    )
  }

  override def assembleFields (key: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
    uuid = Bytes.toString(source.get(("f", "uuid")).get)
    repeats = Bytes.toInt(source.get(("f", "repeats")).get)
  }


  override def structType: StructType = UuidStatSchema.structType

  override def toSparkRow (): Row = {
    Row(uuid, repeats)
  }
}
