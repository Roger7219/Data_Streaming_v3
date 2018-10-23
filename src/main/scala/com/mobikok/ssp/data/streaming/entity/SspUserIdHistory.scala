package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.Map

object SspUserIdHistorySchema {

  val structType = StructType(
    StructField("userid", StringType)::
    StructField("createTime", StringType)
    :: Nil
  )
}


case class SspUserIdHistory(@BeanProperty var userid: String,
                            @BeanProperty var createTime: String
                    ) extends HBaseStorable {

  def this () {
    this(null, null)
  }

  override def toHBaseRowkey: Array[Byte] = {
    if(userid == null ) null else Bytes.toBytes(userid)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
      ("f","userid") -> Bytes.toBytes(userid),
      ("f","createTime") -> Bytes.toBytes(createTime)
    )
  }

  override def assembleFields (key: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
    userid = Bytes.toString(source.get(("f", "userid")).get)
    createTime = Bytes.toString(source.get(("f", "createTime")).get)
  }



  override def structType: StructType = SspUserIdHistorySchema.structType

  override def toSparkRow (): Row = {
    Row(userid, createTime)

  }
}
