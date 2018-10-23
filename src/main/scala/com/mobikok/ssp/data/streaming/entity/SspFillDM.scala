package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty


/**
  *
CREATE TABLE  SSP_FILL_DM_PHOENIX (
    PUBLISHERID  INTEGER NOT NULL,
    SUBID        INTEGER NOT NULL,
    COUNTRYID    INTEGER NOT NULL,
    CARRIERID    INTEGER NOT NULL,
    SV           VARCHAR NOT NULL,

    ADTYPE       INTEGER NOT NULL,
    B_DATE       VARCHAR NOT NULL,
    TIMES        BIGINT,
    L_TIME       VARCHAR,
    --    upsert into 字符串字段值要用单引号！！！
    constraint pk primary key (SUBID,COUNTRYID,CARRIERID,SV,ADTYPE,B_DATE)
);
  */
@deprecated
object SspFillDMSchema{
  val structType = StructType(
      StructField("PUBLISHERID", IntegerType) ::
      StructField("SUBID",      IntegerType)  ::
      StructField("COUNTRYID",  IntegerType)  ::
      StructField("CARRIERID",  IntegerType)  ::
      StructField("SV",         StringType)  ::

      StructField("ADTYPE",     IntegerType)   ::
      StructField("B_DATE",     StringType)   ::
      StructField("TIMES",      LongType)     ::
      StructField("L_TIME",     StringType)  :: Nil
  )
}

// Order: dwrGroupBy b_date aggExprsAlias l_time
@deprecated
case class SspFillDM (
                      @BeanProperty var PUBLISHERID: Int,
                      @BeanProperty var SUBID: Int,
                      @BeanProperty var COUNTRYID: Int,
                      @BeanProperty var CARRIERID: Int,
                      @BeanProperty var SV: String,

                      @BeanProperty var ADTYPE: Int, //+
                      @BeanProperty var B_DATE: String,
                      @BeanProperty var TIMES: Long,
                      @BeanProperty var L_TIME: String
                     ) extends PhoenixStorable {
  def this () {
    this(
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[Int],
      null.asInstanceOf[String],

      null.asInstanceOf[Int],
      null.asInstanceOf[String],
      null.asInstanceOf[Long],
      null.asInstanceOf[String]
    )
  }

  override def toSparkRow (): Row = {
    Row(PUBLISHERID,SUBID, COUNTRYID, CARRIERID, SV,
      ADTYPE, B_DATE, TIMES, L_TIME)
  }

  override def structType = SspFillDMSchema.structType

//  override def setRowkey (key: Array[Byte]): HBaseStorable = {
//    val v = parseRowkey(
//      key,
//      classOf[Int],
//      classOf[Int],
//      classOf[Int],
//      classOf[Int],
//      classOf[String],
//
//      classOf[Int],
//      classOf[String]
//    )
//
//    PUBLISHERID = v(0).asInstanceOf[Int]
//    SUBID = v(1).asInstanceOf[Int]
//    COUNTRYID = v(2).asInstanceOf[Int]
//    CARRIERID = v(3).asInstanceOf[Int]
//    SV = v(4).asInstanceOf[String]
//
//    ADTYPE = v(5).asInstanceOf[Int]
//    B_DATE = v(6).asInstanceOf[String]
//
//    this
//  }
  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {
  val v = parseRowkey(
    row,
    classOf[Int],
    classOf[Int],
    classOf[Int],
    classOf[Int],
    classOf[String],

    classOf[Int],
    classOf[String]
  )

  PUBLISHERID = v(0).asInstanceOf[Int]
  SUBID = v(1).asInstanceOf[Int]
  COUNTRYID = v(2).asInstanceOf[Int]
  CARRIERID = v(3).asInstanceOf[Int]
  SV = v(4).asInstanceOf[String]

  ADTYPE = v(5).asInstanceOf[Int]
  B_DATE = v(6).asInstanceOf[String]

  TIMES = Bytes.toLong(source.get(("0", "TIMES")).get) + Long.MaxValue + 1
    L_TIME = Bytes.toString(source.get(("0", "L_TIME")).get)
  }

  override def toHBaseRowkey: Array[Byte] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't get rowkey(storeKey) by 'PhoenixStorable' instance")
  }

  override def toColumns: collection.Map[(String, String), Array[Byte]] = {
    throw new UnsupportedOperationException("Store use Phoenix API! Don't use the 'PhoenixStorable' instance to save to the phoenix table")
  }

}



// rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
// column values: column=0:_0,timestamp=1498151253737, value=x
//                column=0:L_TIME, timestamp=1498151253737, value=22
//                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01