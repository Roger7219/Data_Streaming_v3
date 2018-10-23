package com.mobikok.ssp.data.streaming.entity

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, JSONSerializable}
import com.mobikok.ssp.data.streaming.util.OM
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty
import scala.collection.Map


object RatingTopNSchema {

  val structType = StructType(
    StructField("user", IntegerType) ::
      StructField("imei", StringType) ::
      StructField("product", IntegerType) ::
      StructField("rating", DoubleType) ::
      Nil
  )
}

case class Rating (@BeanProperty var user: Int,
                   @BeanProperty var imei: String,
                   @BeanProperty var product: Int,
                   @BeanProperty var rating: Double) extends JSONSerializable {

}

/**
  * Created by Administrator on 2017/7/31.
  */
case class RatingTopN (var user: Int, @BeanProperty var ratings: List[Rating]) extends HBaseStorable {

  override def toSparkRow (): Row = Row(ratings)

//  /**
//    * 其实是setRowkey<br>
//    * Example:<br>
//    * RowkeyUuid("id_val","name_val").restoreKey(Bytes.toBytes("id_val"))
//    *
//    * @param key
//    * @return
//    */
//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
//    this.key = Bytes.toString(key)
//    this
//  }

  /**
    * 其实是getRowkey
    *
    * @return
    */
  override def toHBaseRowkey: Array[Byte] = {
    Bytes.toBytes(user)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
      ("f", "ratings") -> Bytes.toBytes(OM.toJOSN(ratings))
    )
  }

  override def structType: StructType = RatingTopNSchema.structType

  override def assembleFields (row: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
    user =  Bytes.toInt(row)
    ratings = OM.toBean(Bytes.toString(source.get(("f", "ratings")).get), new TypeReference[List[Rating]] {})
  }
}
