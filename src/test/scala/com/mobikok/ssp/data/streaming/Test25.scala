package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/23.
  */
case class Test25 (@BeanProperty var APPID: String,
                   @BeanProperty var APPID2: Int,
                   @BeanProperty var TIMES: Double
                 ) extends PhoenixStorable {

  def this () {
    this(
      null.asInstanceOf[String],
      null.asInstanceOf[Int],
      null.asInstanceOf[Double])
  }

  override def toSparkRow (): Row = {
    Row(APPID, APPID2, TIMES)
  }

  override def structType: StructType =   StructType(
    StructField("APPID", StringType) ::
      StructField("APPID2", IntegerType) ::
      StructField("TIMES", DoubleType)
      :: Nil
  )

  // rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
  // column values: column=0:_0,timestamp=1498151253737, value=x
  //                column=0:L_TIME, timestamp=1498151253737, value=22
  //                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01

//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
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
//    this
//  }

  //BigDecimal存储为String类型
  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {

    val v = parseRowkey(
      row,
      classOf[String],
      classOf[Int]
    )

    APPID = v(0).asInstanceOf[String]
    APPID2 = v(1).asInstanceOf[Int]

    TIMES = -(Bytes.toDouble(source.get(("0", "TIMES")).get))
   //  java.math.BigDecimal.valueOf(0).subtract((Bytes.toBigDecimal(source.get(("0", "TIMES")).get)))
  }

  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Don't get rowkey(storeKey) by 'PhoenixStorable' instance")

  }
  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Don't use the 'PhoenixStorable' instance to save to the phoenix table. Use spark phoenix API")
  }

}
