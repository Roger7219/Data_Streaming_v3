package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty

@deprecated
object SspUserKeepDMSchema{
  val structType = StructType(
    StructField("APPID",        IntegerType)  ::
      StructField("COUNTRYID",  IntegerType)  ::
      StructField("CARRIERID",  IntegerType)  ::
      StructField("SV",         StringType)   ::
      StructField("ACTIVEDATE", StringType)   ::
      StructField("B_DATE",     StringType)   ::

      StructField("USERCOUNT",        LongType)     ::
      StructField("FIRSTCOUNT",       LongType)     ::
      StructField("SECONDCOUNT",      LongType)     ::
      StructField("THIRDCOUNT",       LongType)     ::
      StructField("FOURTHCOUNT",      LongType)     ::

      StructField("FIFTHCOUNT",       LongType)     ::
      StructField("SIXTHCOUNT",       LongType)     ::
      StructField("SEVENTHCOUNT",     LongType)     ::
      StructField("FIFTYCOUNT",       LongType)     ::
      StructField("THIRTYCOUNT",      LongType)     ::

      StructField("L_TIME",     StringType)   :: Nil
  )
}

/**
  * Created by Administrator on 2017/6/23.
  */
// Order: dwrGroupBy b_date aggExprsAlias l_time
@deprecated
case class SspUserKeepDM (@BeanProperty var APPID:     Int,
                          @BeanProperty var COUNTRYID: Int,
                          @BeanProperty var CARRIERID: Int,
                          @BeanProperty var SV:        String,
                          @BeanProperty var ACTIVEDATE:String,
                          @BeanProperty var B_DATE:    String,

                          @BeanProperty var USERCOUNT: Long,
                          @BeanProperty var FIRSTCOUNT: Long,
                          @BeanProperty var SECONDCOUNT: Long,
                          @BeanProperty var THIRDCOUNT: Long,
                          @BeanProperty var FOURTHCOUNT: Long,

                          @BeanProperty var FIFTHCOUNT: Long,
                          @BeanProperty var SIXTHCOUNT: Long,
                          @BeanProperty var SEVENTHCOUNT: Long,
                          @BeanProperty var FIFTYCOUNT: Long,
                          @BeanProperty var THIRTYCOUNT: Long,

                          @BeanProperty var L_TIME: String
                     ) extends PhoenixStorable {

  def this () {
    this(
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],

      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],
      null.asInstanceOf[Long],

      null.asInstanceOf[String]
    )
  }

  override def toSparkRow (): Row = {
    Row(APPID, COUNTRYID, CARRIERID, SV, ACTIVEDATE, B_DATE,
      USERCOUNT,FIRSTCOUNT, SECONDCOUNT, THIRDCOUNT, FOURTHCOUNT,
      FIFTHCOUNT,SIXTHCOUNT,SEVENTHCOUNT, FIFTYCOUNT,THIRTYCOUNT,
      L_TIME)
  }

  override def structType: StructType = SspUserKeepDMSchema.structType

//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
//    val v = parseRowkey(
//      key,
//      classOf[Int],
//      classOf[Int],
//      classOf[Int],
//      classOf[String],
//      classOf[String],
//      classOf[String]
//    )
//
//      APPID = v(0).asInstanceOf[Int]
//      COUNTRYID = v(1).asInstanceOf[Int]
//      CARRIERID = v(2).asInstanceOf[Int]
//      SV = v(3).asInstanceOf[String]
//      ACTIVEDATE = v(4).asInstanceOf[String]
//      B_DATE = v(5).asInstanceOf[String]
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
      classOf[String],
      classOf[String]
    )

    APPID = v(0).asInstanceOf[Int]
    COUNTRYID = v(1).asInstanceOf[Int]
    CARRIERID = v(2).asInstanceOf[Int]
    SV = v(3).asInstanceOf[String]
    ACTIVEDATE = v(4).asInstanceOf[String]
    B_DATE = v(5).asInstanceOf[String]

    USERCOUNT = Bytes.toLong(source.get(("0", "USERCOUNT")).get) + Long.MaxValue + 1
    FIRSTCOUNT = Bytes.toLong(source.get(("0", "FIRSTCOUNT")).get) + Long.MaxValue + 1
    SECONDCOUNT = Bytes.toLong(source.get(("0", "SECONDCOUNT")).get) + Long.MaxValue + 1
    THIRDCOUNT = Bytes.toLong(source.get(("0", "THIRDCOUNT")).get) + Long.MaxValue + 1
    FOURTHCOUNT = Bytes.toLong(source.get(("0", "FOURTHCOUNT")).get) + Long.MaxValue + 1
    FIFTHCOUNT = Bytes.toLong(source.get(("0", "FIFTHCOUNT")).get) + Long.MaxValue + 1
    SIXTHCOUNT = Bytes.toLong(source.get(("0", "SIXTHCOUNT")).get) + Long.MaxValue + 1
    SEVENTHCOUNT = Bytes.toLong(source.get(("0", "SEVENTHCOUNT")).get) + Long.MaxValue + 1
    FIFTYCOUNT = Bytes.toLong(source.get(("0", "FIFTYCOUNT")).get) + Long.MaxValue + 1
    THIRTYCOUNT = Bytes.toLong(source.get(("0", "THIRTYCOUNT")).get) + Long.MaxValue + 1
    L_TIME = Bytes.toString(source.get(("0", "L_TIME")).get)
  }

  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't get rowkey(storeKey) by 'PhoenixStorable' instance")
  }

  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't use the 'PhoenixStorable' instance to save to the phoenix table")
  }

}
