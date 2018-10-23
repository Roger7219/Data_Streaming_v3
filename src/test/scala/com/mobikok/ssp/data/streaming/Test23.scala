package com.mobikok.ssp.data.streaming


import java.util

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/23.
  */
case class Test23(@BeanProperty var APPID: String,
                  @BeanProperty var APPID2: String,
                  @BeanProperty var TIMES: Int
                 ) extends PhoenixStorable {

  def this () {
    this(
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Int])
  }

  override def toSparkRow (): Row = {
    Row(APPID, APPID2, TIMES)
  }

  override def structType: StructType =   StructType(
    StructField("APPID", StringType) ::
      StructField("APPID2", StringType) ::
      StructField("TIMES", IntegerType)
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
////    val fields = new java.util.ArrayList[Array[Byte]](5)
////    for (i <- 0 until key.length) {
////      end = i
////      val cvv=key(i)
////
//////      println("============" + key(i) + "   " +(cvv == ('\u0000')))
////      if (key(i) == '\u0000') {
////        end = i
////        fields.add(java.util.Arrays .copyOfRange(key, start, end))
////        start = i + 1
////
////      }
////    }
////    //latest
////    fields.add(java.util.Arrays.copyOfRange(key, start, key.length))
////
////    APPID = Bytes.toString(fields.get(0))
////    APPID2 = Bytes.toString(fields.get(1))
//
//    val v = parseRowkey(
//      key,
//      classOf[String],
//      classOf[String]
//    )
//
//    APPID = v(0).asInstanceOf[String]
//    APPID2 = v(1).asInstanceOf[String]
//
//    this
//  }
//  override def mappingFields (source: collection.Map[(String, String), Array[Byte]]): Unit = {
//    TIMES = Integer.MAX_VALUE + Bytes.toInt(source.get(("0", "TIMES")).get) + 1
//  }


  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Don't get rowkey(storeKey) by 'PhoenixStorable' instance")
  }

  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Don't use the 'PhoenixStorable' instance to save to the phoenix table. Use spark phoenix API")
  }

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {

    //要把setRowkey 的逻辑加进来

    TIMES = Integer.MAX_VALUE + Bytes.toInt(source.get(("0", "TIMES")).get) + 1
  }
}

object s{
  def main (args: Array[String]): Unit = {
    val e= -1212121211 >>> 32
    java.util.Arrays.deepToString(Array())
    println(e)
  }
}