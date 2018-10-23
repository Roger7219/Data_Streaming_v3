package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util._

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/23.
  */
case class Test24 (@BeanProperty var APPID: String,
                   @BeanProperty var APPID2: Int,
                   @BeanProperty var TIMES: Int
                 ) extends PhoenixStorable {

  def this () {
    this(
      null.asInstanceOf[String],
      null.asInstanceOf[Int],
      null.asInstanceOf[Int])
  }

  override def toSparkRow (): Row = {
    Row(APPID, APPID2, TIMES)
  }

  override def structType: StructType =   StructType(
    StructField("APPID", StringType) ::
      StructField("APPID2", IntegerType) ::
      StructField("TIMES", LongType)
      :: Nil
  )

  // rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
  // column values: column=0:_0,timestamp=1498151253737, value=x
  //                column=0:L_TIME, timestamp=1498151253737, value=22
  //                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01

//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
////    var start = 0
////    var end = 0
////    //    var foundStart = false
////    val fields = new ArrayList[Array[Byte]](5)
////    for (i <- 0 until key.length) {
////      end = i
////      val cvv=key(i)
////
////      if (key(i) == '\u0000') {
////        end = i
////        fields.add(Arrays.copyOfRange(key, start, end))
////        start = i + 1
////
////      }
////    }
////    //latest
////    fields.add(Arrays.copyOfRange(key, start, key.length))
////
//
//    val v = parseRowkey(
//      key,
//      classOf[String],
//      classOf[Int]
//    )
//
//    APPID = v(0).asInstanceOf[String]
//    APPID2 = v(1).asInstanceOf[Int]
//
////    APPID = Bytes.toString(fields.get(0))
////    APPID2 = Bytes.toInt(fields.get(1))  // +Int.MaxValue + 1
//    this
//  }

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {
    val v = parseRowkey(
      row,
      classOf[String],
      classOf[Int]
    )

    APPID = v(0).asInstanceOf[String]
    APPID2 = v(1).asInstanceOf[Int]

    TIMES = Bytes.toInt(source.get("0", "TIMES").get)+Int.MaxValue + 1
  }

  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Don't get rowkey(storeKey) by 'PhoenixStorable' instance")
  }

  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Don't use the 'PhoenixStorable' instance to save to the phoenix table. Use spark phoenix API")
  }

}
