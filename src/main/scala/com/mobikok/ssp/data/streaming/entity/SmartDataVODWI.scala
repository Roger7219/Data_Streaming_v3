package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.PhoenixStorable
import com.mobikok.ssp.data.streaming.schema.dwi.dao.SmartDataVODWISchema
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

import scala.beans.BeanProperty
import scala.collection.Map


// Order: dwrGroupBy b_date aggExprsAlias l_time
case class  SmartDataVODWI (
                       @BeanProperty var repeats:      Integer,
                       @BeanProperty var rowkey:       String,

                       @BeanProperty var id:           Integer,
                       @BeanProperty var campaignId:   Integer,
                       @BeanProperty var s1:           String,
                       @BeanProperty var s2:           String,
                       @BeanProperty var createTime:   String,

                       @BeanProperty var offerId:      Integer,
                       @BeanProperty var countryId:    Integer,
                       @BeanProperty var carrierId:    Integer,
                       @BeanProperty var deviceType:   Integer,
                       @BeanProperty var userAgent:    String,

                       @BeanProperty var ipAddr:       String,
                       @BeanProperty var clickId:      String,
                       @BeanProperty var price:        Double,
                       @BeanProperty var status:       Integer,
                       @BeanProperty var sendStatus:   Integer,

                       @BeanProperty var reportTime:   String,
                       @BeanProperty var sendPrice:    Double,
                       @BeanProperty var frameId:      Integer,
                       @BeanProperty var referer:      String,
                       @BeanProperty var isTest:       Integer,

                       @BeanProperty var times:        Integer,
                       @BeanProperty var res:          String,
                       @BeanProperty var `type`:       String,
                       @BeanProperty var clickUrl:     String,
                       @BeanProperty var reportIp:     String,

                       @BeanProperty var repeated:     String,
                       @BeanProperty var l_time:       String,
                       @BeanProperty var b_date:       String
                      ) extends PhoenixStorable {

  override def toSparkRow (): Row = {
    Row(repeats, rowkey,
        id,         campaignId, s1,         s2,         createTime,
        offerId,    countryId,  carrierId,  deviceType, userAgent,
        ipAddr,     clickId,    price,      status,     sendStatus,
        reportTime, sendPrice,  frameId,    referer,    isTest,
        times,      res,        `type`,     clickUrl,   reportIp,
        repeated,  l_time, b_date)
  }

  override def structType: StructType = SmartDataVODWISchema.structType

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {

    repeats    = getInt(source, "repeats") //ifNull(source.get(("0","repeats")).get, null,  Bytes.toInt(source.get(("0","repeats")).get) + Int.MaxValue + 1)
    rowkey     = getStr(source, "rowkey") //ifNull(source.get(("0","rowkey")).get, null, Bytes.toString(source.get(("0","rowkey")).get))

    id         = getInt(source, "id") //ifNull(source.get(("0","id")).get, null,  Bytes.toInt(source.get(("0","id")).get) + Int.MaxValue + 1)
    campaignId = getInt(source, "campaignId") // ifNull(source.get(("0","campaignId")).get, null,  Bytes.toInt(source.get(("0","campaignId")).get) + Int.MaxValue + 1)
    s1         = getStr(source, "s1")     // ifNull(source.get(("0","s1")).get, null,  Bytes.toString(source.get(("0","s1")).get))
    s2         = getStr(source, "s2")    // ifNull(source.get(("0","s2")).get, null,  Bytes.toString(source.get(("0","s2")).get))
    createTime = getStr(source, "createTime") // ifNull(source.get(("0","createTime")).get, null,  Bytes.toString(source.get(("0","createTime")).get))

    offerId    = getInt(source, "offerId") // ifNull(source.get(("0","offerId")).get, null,  Bytes.toInt(source.get(("0","offerId")).get) + Int.MaxValue + 1)
    countryId  = getInt(source, "countryId") // ifNull(source.get(("0","countryId")).get, null,  Bytes.toInt(source.get(("0","countryId")).get) + Int.MaxValue + 1)
    carrierId  = getInt(source, "carrierId") // ifNull(source.get(("0","carrierId")).get, null,  Bytes.toInt(source.get(("0","carrierId")).get) + Int.MaxValue + 1)
    deviceType = getInt(source, "deviceType")// ifNull(source.get(("0","deviceType")).get, null,  Bytes.toInt(source.get(("0","deviceType")).get) + Int.MaxValue + 1)
    userAgent  = getStr(source, "userAgent") // ifNull(source.get(("0","userAgent")).get, null,  Bytes.toString(source.get(("0","userAgent")).get))

    ipAddr     = getStr(source, "ipAddr") // ifNull(source.get(("0","ipAddr")).get, null,  Bytes.toString(source.get(("0","ipAddr")).get))
    clickId    = getStr(source, "clickId")//ifNull(source.get(("0","clickId")).get, null,  Bytes.toString(source.get(("0","clickId")).get) )
    price      = getDou(source, "price") //ifNull(source.get(("0","price")).get, null,  java.lang.Double.valueOf( Bytes.toString(source.get(("0","price")).get)))
    status     = getInt(source, "status") //ifNull(source.get(("0","status")).get, null,  Bytes.toInt(source.get(("0","status")).get) + Int.MaxValue + 1)
    sendStatus = getInt(source, "sendStatus") //ifNull(source.get(("0","sendStatus")).get, null,  Bytes.toInt(source.get(("0","sendStatus")).get) + Int.MaxValue + 1)

    reportTime = getStr(source, "reportTime")// ifNull(source.get(("0","reportTime")).get, null,  Bytes.toString(source.get(("0","reportTime")).get))
    sendPrice  = getDou(source, "sendPrice") //ifNull(source.get(("0","sendPrice")).get, null,  java.lang.Double.valueOf( Bytes.toString(source.get(("0","sendPrice")).get)))
    frameId    = getInt(source, "frameId") //ifNull(source.get(("0","frameId")).get, null,  Bytes.toInt(source.get(("0","frameId")).get) + Int.MaxValue + 1)
    referer    = getStr(source, "referer") //ifNull(source.get(("0","referer")).get, null,  Bytes.toString(source.get(("0","referer")).get))
    isTest     = getInt(source, "isTest")

    times      = getInt(source, "times")
    res        = getStr(source, "res")
    `type`     = getStr(source, "type")
    clickUrl   = getStr(source, "clickUrl")
    reportIp   = getStr(source, "reportIp")

    repeated   = getStr(source, "repeated") //ifNull(source.get(("0","repeated")).get, null,  Bytes.toString(source.get(("0","repeated")).get))
    l_time     = getStr(source, "l_time")   //ifNull(source.get(("0","l_time")).get, null,   Bytes.toString(source.get(("0","l_time")).get))
    b_date     = getStr(source, "b_date")   //ifNull(source.get(("0","b_date")).get, null,   Bytes.toString(source.get(("0","b_date")).get))
  }

  //保存
  override def toHBaseRowkey: Array[Byte] = {
    if(clickId == null ) null else Bytes.toBytes(clickId)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(

      ("0","repeats")         ->    setInt(repeats), //ifNull(repeats, null, Bytes.toBytes(repeats - Int.MaxValue - 1)),
      ("0","rowkey")          ->    setStr(rowkey),//ifNull(rowkey, null, Bytes.toBytes(rowkey)),

      ("0", "id")             ->    setInt(id),
      ("0", "campaignId")     ->    setInt(campaignId),
      ("0", "s1")             ->    setStr(s1),
      ("0", "s2")             ->    setStr(s2),
      ("0", "createTime")     ->    setStr(createTime),
      ("0", "offerId")        ->    setInt(offerId),
      ("0", "countryId")      ->    setInt(countryId),
      ("0", "carrierId")      ->    setInt(carrierId),
      ("0", "deviceType")     ->    setInt(deviceType),
      ("0", "userAgent")      ->    setStr(userAgent),
      ("0", "ipAddr")         ->    setStr(ipAddr),
      ("0", "clickId")        ->    setStr(clickId),
      ("0", "price")          ->    setDou(price),
      ("0", "status")         ->    setInt(status),
      ("0", "sendStatus")     ->    setInt(sendStatus),
      ("0", "reportTime")     ->    setStr(reportTime),
      ("0", "sendPrice")      ->    setDou(sendPrice),
      ("0", "frameId")        ->    setInt(frameId),
      ("0", "referer")        ->    setStr(referer),
      ("0", "isTest")         ->    setInt(isTest),
      ("0", "times")          ->    setInt(times),
      ("0", "res")            ->    setStr(res),
      ("0", "type")           ->    setStr(`type`),
      ("0", "clickUrl")       ->    setStr(clickUrl),
      ("0", "reportIp")       ->    setStr(reportIp),

      ("0", "repeated")       ->    setStr(repeated),
      ("0", "l_time")         ->    setStr(l_time),
      ("0", "b_date")         ->    setStr(b_date)

    )
  }


  def this () {
    this(
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],

      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Double],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[Double],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],

      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String]
    )
  }


}















//
//object O {
//
//  def ifNull[T] (o: Array[Byte], a:T, b:T): T = {
//    if(o == null) a else b
//  }
//
//  def main (args: Array[String]): Unit = {
//
//    ifNull(null,null, null)
//
//
//
//
//    println(new ObjectMapper().writeValueAsString(Bytes.toBytes("11" + "\u0000")))
//
////    println(new ObjectMapper().readValue("MTE=", new TypeReference[Array[Byte]](){}))
//
//
////    println(new ObjectMapper().readValue("MTE=", classOf[Object]))
//
//
//    //   println(Bytes.toBytes("11" + "\u0000").toString)
//
//    val c = Class.forName("com.mobikok.ssp.data.streaming.entity.SspClikDWI")
//    val hashMap = new java.util.HashMap[String, Object](){{
//      put("id", new Integer(11))
//      put("clickId", new Integer(11))
//
//      put("sendStatus", "sendStatus_val")
//    }}
//
//    val cc= OM.convert(hashMap, c)
//    println(cc)
//
//  }
//}




// rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
// column values: column=0:_0,timestamp=1498151253737, value=x
//                column=0:L_TIME, timestamp=1498151253737, value=22
//                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01


