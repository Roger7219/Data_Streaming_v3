package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.Map

/**
  * Created by admin on 2017/8/31.
  */

object PvIpStatSchema {

  val structType = StructType(
      StructField("pv", StringType) ::
      StructField("uv", StringType) ::
      StructField("stayTime",IntegerType) ::

      StructField("ip", StringType) :: Nil
  )
}

case class PvIpStat(@BeanProperty var pv: Integer, @BeanProperty var uv: Integer, @BeanProperty var stayTime: Integer, @BeanProperty var ip: String
) extends HBaseStorable {
  def this () {
    this(
        null.asInstanceOf[Int],
        null.asInstanceOf[Integer],
        null.asInstanceOf[Integer],
        null.asInstanceOf[String]
    )
  }

  override def toHBaseRowkey: Array[Byte] = {
    if(ip == null ) null else Bytes.toBytes(ip)
  }

  def ifNull (o: Any, a: => Array[Byte], b: => Array[Byte]): Array[Byte] = {
    if(o == null)  a else b
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(
      ("f","pv") -> ifNull(pv, null, Bytes.toBytes(pv - Int.MaxValue - 1)),
      ("f","uv") -> ifNull(pv, null, Bytes.toBytes(uv - Int.MaxValue - 1)),
      ("f","stayTime") -> ifNull(pv, null, Bytes.toBytes(stayTime - Int.MaxValue - 1)),
      ("f","ip") -> ifNull(pv, null, Bytes.toBytes(ip))
    )
  }

  def ifNull[T] (o: Array[Byte], a: => T, b: => T): T = {
    if(o == null) a else b
  }

  override def assembleFields (key: Array[Byte], source: Map[(String, String), Array[Byte]]): Unit = {
    pv = ifNull(source.get((("f", "pv"))).get, null,  Bytes.toInt(source.get((("f", "pv"))).get) + Int.MaxValue + 1)

    uv = ifNull(source.get((("f", "uv"))).get, null,  Bytes.toInt(source.get((("f", "uv"))).get) + Int.MaxValue + 1)
    stayTime = ifNull(source.get((("f", "stayTime"))).get, null,  Bytes.toInt(source.get((("f", "stayTime"))).get) + Int.MaxValue + 1)
    ip = ifNull(source.get((("f", "ip"))).get, null,  Bytes.toString(source.get((("f", "ip"))).get))
  }


  override def structType: StructType = PvIpStatSchema.structType

  override def toSparkRow (): Row = {
    Row(pv, ip, stayTime, ip)
  }
}
