package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty

@deprecated
object SspUserNewAndActiveDMSchema{

  val structType = StructType(
    StructField("APPID",          IntegerType)  ::
      StructField("COUNTRYID",    IntegerType)  ::
      StructField("CARRIERID",    IntegerType)  ::
      StructField("SV",           StringType)   ::
      StructField("B_DATE",       StringType)   ::

      StructField("NEWCOUNT",     LongType)     ::
      StructField("ACTIVECOUNT",  LongType)     ::
      StructField("L_TIME",       StringType)  :: Nil
  )
}

/**
  * Created by Administrator on 2017/6/23.
  */
@deprecated
case class SspUserNewAndActiveDM (@BeanProperty var APPID: Int,
                                  @BeanProperty var COUNTRYID: Int,
                                  @BeanProperty var CARRIERID: Int,
                                  @BeanProperty var SV: String,
                                  @BeanProperty var B_DATE: String,

                                  @BeanProperty var NEWCOUNT: Long,
                                  @BeanProperty var ACTIVECOUNT: Long,
                                  @BeanProperty var L_TIME: String
                     ) extends PhoenixStorable {

  def this () {
    this(
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[String])
  }


  override def toSparkRow (): Row = {
    Row(APPID, COUNTRYID, CARRIERID, SV, B_DATE,
      NEWCOUNT, ACTIVECOUNT, L_TIME)
  }

  override def structType: StructType = SspUserNewAndActiveDMSchema.structType

  // rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
  // column values: column=0:_0,timestamp=1498151253737, value=x
  //                column=0:L_TIME, timestamp=1498151253737, value=22
  //                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01


//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
//    val v = parseRowkey(
//      key,
//      classOf[Int],
//      classOf[Int],
//      classOf[Int],
//      classOf[String],
//      classOf[String]
//    )
//
//      APPID = v(0).asInstanceOf[Int]
//      COUNTRYID = v(1).asInstanceOf[Int]
//      CARRIERID = v(2).asInstanceOf[Int]
//      SV = v(3).asInstanceOf[String]
//      B_DATE = v(4).asInstanceOf[String]
//
//    this
//  }

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {
    val v = parseRowkey(
      row,
      classOf[Int],
      classOf[Int],
      classOf[Int],
      classOf[String],
      classOf[String]
    )

    APPID = v(0).asInstanceOf[Int]
    COUNTRYID = v(1).asInstanceOf[Int]
    CARRIERID = v(2).asInstanceOf[Int]
    SV = v(3).asInstanceOf[String]
    B_DATE = v(4).asInstanceOf[String]

    NEWCOUNT = Bytes.toLong(source.get(("0", "NEWCOUNT")).get) + Long.MaxValue + 1
    ACTIVECOUNT = Bytes.toLong(source.get(("0", "ACTIVECOUNT")).get) + Long.MaxValue + 1
    L_TIME = Bytes.toString(source.get(("0", "L_TIME")).get)
  }

  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't get rowkey(storeKey) by 'PhoenixStorable' instance")
  }

  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't use the 'PhoenixStorable' instance to save to the phoenix table")
  }

}
