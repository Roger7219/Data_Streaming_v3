package com.mobikok.ssp.data.streaming.entity

import java.util.UUID

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.ssp.data.streaming.entity.feature.PhoenixStorable
import com.mobikok.ssp.data.streaming.schema.dwi.AggTrafficDWISchema
import com.mobikok.ssp.data.streaming.util.OM
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty
import scala.collection.Map

// Order: dwrGroupBy b_date aggExprsAlias l_time
case class  AggTrafficDWI (
                       @BeanProperty var repeats:     Integer,
                       @BeanProperty var rowkey:      String,

                       @BeanProperty var appId:       Integer,
                       @BeanProperty var jarId:       Integer,
                       @BeanProperty var jarIds:      Array[Integer],
                       @BeanProperty var publisherId: Integer,
                       @BeanProperty var imei:        String,

                       @BeanProperty var imsi:        String,
                       @BeanProperty var version:     String,
                       @BeanProperty var model:       String,
                       @BeanProperty var screen:      String,
                       @BeanProperty var installType: Integer,

                       @BeanProperty var sv:          String,
                       @BeanProperty var leftSize:    String,
                       @BeanProperty var androidId:   String,
                       @BeanProperty var userAgent:   String,
                       @BeanProperty var connectType: Integer,

                       @BeanProperty var createTime:  String,
                       @BeanProperty var clickTime:   String,
                       @BeanProperty var showTime:    String,
                       @BeanProperty var reportTime:  String,
                       @BeanProperty var countryId:   Integer,

                       @BeanProperty var carrierId:   Integer,
                       @BeanProperty var ipAddr:      String,
                       @BeanProperty var deviceType:  Integer,
                       @BeanProperty var pkgName:     String,
                       @BeanProperty var s1:          String,

                       @BeanProperty var s2:          String,
                       @BeanProperty var clickId:     String,
                       @BeanProperty var reportPrice: java.lang.Double,
                       @BeanProperty var pos:         Integer,
                       @BeanProperty var affSub:      String,

                       @BeanProperty var repeated:    String,
                       @BeanProperty var l_time:      String,
                       @BeanProperty var b_date:      String
                      ) extends PhoenixStorable {

  override def toSparkRow (): Row = {
    Row(repeats, rowkey,
      appId,      jarId,     jarIds,      publisherId, imei,
      imsi,       version,   model,       screen,      installType,
      sv,         leftSize,  androidId,   userAgent,   connectType,
      createTime, clickTime, showTime,    reportTime,  countryId,
      carrierId,  ipAddr,    deviceType,  pkgName,     s1,
      s2,         clickId,   reportPrice, pos,         affSub,
      repeated,  l_time, b_date)
  }

  override def structType: StructType = AggTrafficDWISchema.structType

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {
//    val v = parseRowkey(
//      row,
//      classOf[String]
//    )

    repeats = ifNull(source.get(("0","repeats")), null,  Bytes.toInt(source.get(("0","repeats")).get) + Int.MaxValue + 1)
    rowkey = ifNull(source.get(("0","rowkey")), null, Bytes.toString(source.get(("0","rowkey")).get))

    appId =  ifNull(source.get(("0","appId")), null,  Bytes.toInt(source.get(("0","appId")).get) + Int.MaxValue + 1)
    jarId =       ifNull(source.get(("0","jarId")), null,  Bytes.toInt(source.get(("0","jarId")).get) + Int.MaxValue + 1)
    jarIds = ifNull(source.get(("0","jarIds")), null,  OM.toBean(Bytes.toString(source.get(("0","jarIds")).get), new TypeReference[Array[Integer]] {} ) )
    publisherId = ifNull(source.get(("0","publisherId")), null,  Bytes.toInt(source.get(("0","publisherId")).get) + Int.MaxValue + 1)
    imei = ifNull(source.get(("0","imei")), null,  Bytes.toString(source.get(("0","imei")).get))

    imsi = ifNull(source.get(("0","imsi")), null,  Bytes.toString(source.get(("0","imsi")).get))
    version = ifNull(source.get(("0","version")), null,  Bytes.toString(source.get(("0","version")).get))
    model = ifNull(source.get(("0","model")), null,  Bytes.toString(source.get(("0","model")).get))
    screen = ifNull(source.get(("0","screen")), null,  Bytes.toString(source.get(("0","screen")).get))
    installType =  ifNull(source.get(("0","installType")), null,  Bytes.toInt(source.get(("0","installType")).get) + Int.MaxValue + 1)

    sv = ifNull(source.get(("0","sv")), null,  Bytes.toString(source.get(("0","sv")).get))
    leftSize = ifNull(source.get(("0","leftSize")), null,  Bytes.toString(source.get(("0","leftSize")).get))
    androidId = ifNull(source.get(("0","androidId")), null,  Bytes.toString(source.get(("0","androidId")).get))
    userAgent = ifNull(source.get(("0","userAgent")), null,  Bytes.toString(source.get(("0","userAgent")).get))
    connectType = ifNull(source.get(("0","connectType")), null,  Bytes.toInt(source.get(("0","connectType")).get) + Int.MaxValue + 1)

    createTime =  ifNull(source.get(("0","createTime")), null,  Bytes.toString(source.get(("0","createTime")).get))
    clickTime =  ifNull(source.get(("0","clickTime")), null,  Bytes.toString(source.get(("0","clickTime")).get))
    showTime =  ifNull(source.get(("0","showTime")), null,  Bytes.toString(source.get(("0","showTime")).get))
    reportTime =  ifNull(source.get(("0","reportTime")), null,  Bytes.toString(source.get(("0","reportTime")).get))
    countryId =  ifNull(source.get(("0","countryId")), null,  Bytes.toInt(source.get(("0","countryId")).get) + Int.MaxValue + 1)

    carrierId =  ifNull(source.get(("0","carrierId")), null,  Bytes.toInt(source.get(("0","carrierId")).get) + Int.MaxValue + 1)
    ipAddr = ifNull(source.get(("0","ipAddr")), null,  Bytes.toString(source.get(("0","ipAddr")).get))
    deviceType = ifNull(source.get(("0","deviceType")), null,  Bytes.toInt(source.get(("0","deviceType")).get) + Int.MaxValue + 1)
    pkgName = ifNull(source.get(("0","pkgName")), null,  Bytes.toString(source.get(("0","pkgName")).get))
    s1 = ifNull(source.get(("0","s1")), null,  Bytes.toString(source.get(("0","s1")).get))

    s2 = ifNull(source.get(("0","s2")), null,  Bytes.toString(source.get(("0","s2")).get))
    clickId = ifNull(source.get(("0","clickId")), null,  Bytes.toString(source.get(("0","clickId")).get))
    reportPrice = ifNull(source.get(("0","reportPrice")), null,  java.lang.Double.valueOf( Bytes.toString(source.get(("0","reportPrice")).get)))
    pos = ifNull(source.get(("0","pos")), null,  Bytes.toInt(source.get(("0","pos")).get) + Int.MaxValue + 1)
    affSub = ifNull(source.get(("0","affSub")), null,  Bytes.toString(source.get(("0","affSub")).get))


    repeated = ifNull(source.get(("0","repeated")), null,  Bytes.toString(source.get(("0","repeated")).get))
    l_time = ifNull(source.get(("0","l_time")), null,   Bytes.toString(source.get(("0","l_time")).get))
    b_date = ifNull(source.get(("0","b_date")), null,   Bytes.toString(source.get(("0","b_date")).get))
  }

  //保存
  override def toHBaseRowkey: Array[Byte] = {
    if(clickId == null ) Bytes.toBytes("null_" + UUID.randomUUID().toString) else Bytes.toBytes(clickId)
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(

      ("0","repeats") ->    ifNull(repeats, null, Bytes.toBytes(repeats - Int.MaxValue - 1)),
      ("0","rowkey") ->     ifNull(rowkey, null, Bytes.toBytes(rowkey)),

      ("0","appId") ->       ifNull(appId, null, Bytes.toBytes(appId - Int.MaxValue - 1)),
      ("0","jarId") ->       ifNull(jarId, null, Bytes.toBytes(jarId - Int.MaxValue - 1)),
      ("0","jarIds") ->      ifNull(jarIds, null, Bytes.toBytes(OM.toJOSN(jarIds, false))),
      ("0","publisherId") -> ifNull(publisherId, null, Bytes.toBytes(publisherId - Int.MaxValue - 1)),
      ("0","imei") ->        ifNull(imei, null, Bytes.toBytes(imei)),

      ("0","imsi") ->        ifNull(imsi, null, Bytes.toBytes(imsi)),
      ("0","version") ->     ifNull(version, null, Bytes.toBytes(version)),
      ("0","model") ->       ifNull(model, null, Bytes.toBytes(model)),
      ("0","screen") ->      ifNull(screen, null, Bytes.toBytes(screen)),
      ("0","installType") -> ifNull(installType, null, Bytes.toBytes(installType - Int.MaxValue - 1)),

      ("0","sv") ->          ifNull(sv, null, Bytes.toBytes(sv)),
      ("0","leftSize") ->    ifNull(leftSize, null, Bytes.toBytes(leftSize)),
      ("0","androidId") ->   ifNull(androidId, null, Bytes.toBytes(androidId)),
      ("0","userAgent") ->   ifNull(userAgent, null, Bytes.toBytes(userAgent)),
      ("0","connectType") -> ifNull(connectType, null, Bytes.toBytes(connectType - Int.MaxValue - 1)),

      ("0","createTime") -> ifNull(createTime, null, Bytes.toBytes(createTime)),
      ("0","clickTime") -> ifNull(clickTime, null, Bytes.toBytes(clickTime)),
      ("0","showTime") -> ifNull(showTime, null, Bytes.toBytes(showTime)),
      ("0","reportTime") -> ifNull(reportTime, null, Bytes.toBytes(reportTime)),
      ("0","countryId") -> ifNull(countryId, null, Bytes.toBytes(countryId - Int.MaxValue - 1)),

      ("0","carrierId") -> ifNull(carrierId, null, Bytes.toBytes(carrierId - Int.MaxValue - 1)),
      ("0","ipAddr") -> ifNull(ipAddr, null, Bytes.toBytes(ipAddr)),
      ("0","deviceType") ->  ifNull(deviceType, null, Bytes.toBytes(deviceType - Int.MaxValue - 1)),
      ("0","pkgName") ->  ifNull(pkgName, null, Bytes.toBytes(pkgName)),
      ("0","s1") ->  ifNull(s1, null, Bytes.toBytes(s1)),

      ("0","s2") ->  ifNull(s2, null, Bytes.toBytes(s2)),
      ("0","clickId") ->  ifNull(clickId, null, Bytes.toBytes(clickId)),
      ("0","reportPrice") -> ifNull(reportPrice, null, Bytes.toBytes(reportPrice + "")),
      ("0","pos") ->  ifNull(pos, null, Bytes.toBytes(pos - Int.MaxValue - 1)),
      ("0","affSub") ->  ifNull(affSub, null, Bytes.toBytes(affSub)),

      ("0","repeated") -> ifNull(repeated, null, Bytes.toBytes(repeated)),
      ("0","l_time") ->   ifNull(l_time, null, Bytes.toBytes(l_time)),
      ("0","b_date") ->   ifNull(b_date, null, Bytes.toBytes(b_date))
    )
  }


  def this () {
    this(
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],


      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Array[Integer]],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],

      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String]
    )
  }


}





















// rowkey: \x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01\x80\x00\x00\x01test_row_1
// column values: column=0:_0,timestamp=1498151253737, value=x
//                column=0:L_TIME, timestamp=1498151253737, value=22
//                column=0:TIMES, timestamp=1498151253737, value=\x80\x00\x00\x00\x00\x00\x00\x01


