package com.mobikok.ssp.data.streaming.entity

import co.cask.tephra.distributed.thrift.TTransactionServer.Processor.status
import com.fasterxml.jackson.databind.ObjectMapper
import com.mobikok.ssp.data.streaming.entity.feature.{HBaseStorable, PhoenixStorable}
import com.mobikok.ssp.data.streaming.schema.dwi.dao.SspTrafficDWISchema
import com.mobikok.ssp.data.streaming.util.OM
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}
import org.codehaus.jackson.`type`.TypeReference

import scala.beans.BeanProperty
import scala.collection.{Map, mutable}


// Order: dwrGroupBy b_date aggExprsAlias l_time
case class  SspTrafficDWI (
                       @BeanProperty var repeats:     Integer,
                       @BeanProperty var rowkey:      String,

                       @BeanProperty var id:          Integer,
                       @BeanProperty var publisherId: Integer,
                       @BeanProperty var subId:       Integer,
                       @BeanProperty var offerId:     Integer,
                       @BeanProperty var campaignId:  Integer,
                       @BeanProperty var countryId:   Integer,
                       @BeanProperty var carrierId:   Integer,
                       @BeanProperty var deviceType:  Integer,
                       @BeanProperty var userAgent:   String,
                       @BeanProperty var ipAddr:      String,
                       @BeanProperty var clickId:     String,
                       @BeanProperty var price:       java.lang.Double,
                       @BeanProperty var reportTime:  String,
                       @BeanProperty var createTime:  String,
                       @BeanProperty var clickTime:   String,
                       @BeanProperty var showTime:    String,
                       @BeanProperty var requestType: String,
                       @BeanProperty var priceMethod: Integer,
                       @BeanProperty var bidPrice:    java.lang.Double,
                       @BeanProperty var adType:      Integer,
                       @BeanProperty var isSend:      Integer,
                       @BeanProperty var reportPrice: java.lang.Double,
                       @BeanProperty var sendPrice:   java.lang.Double,
                       @BeanProperty var s1:          String,
                       @BeanProperty var s2:          String,
                       @BeanProperty var gaid:        String,
                       @BeanProperty var androidId:   String,
                       @BeanProperty var idfa:        String,
                       @BeanProperty var postBack:    String,
                       @BeanProperty var sendStatus:  Integer,
                       @BeanProperty var sendTime:    String,
                       @BeanProperty var sv:          String,
                       @BeanProperty var imei:        String,
                       @BeanProperty var imsi:        String,
                       @BeanProperty var imageId:     Integer,
                       @BeanProperty var affSub:      String,             // 子渠道
                       @BeanProperty var s3:          String,
                       @BeanProperty var s4:          String,
                       @BeanProperty var s5:          String,

                       @BeanProperty var packageName: String,             //包名
                       @BeanProperty var domain:      String,             //域名
                       @BeanProperty var respStatus:  Integer,            //未下发原因
                       @BeanProperty var winPrice:    java.lang.Double,   //中签价格
                       @BeanProperty var winTime:     String,             //中签时间

                       @BeanProperty var appPrice:    java.lang.Double,   //app价格 Dsp V1.0.1 新增
                       @BeanProperty var test:        Integer,            //Dsp A/B测试 Dsp V1.0.1 新增
                       @BeanProperty var ruleId:      Integer,            //smartLink + 新增
                       @BeanProperty var smartId:     Integer,
                       @BeanProperty var reportIp:    String,

                       @BeanProperty var pricePercent:   Integer, //单价比例
                       @BeanProperty var appPercent:     Integer, //app比例
                       @BeanProperty var salePercent:    Integer, //按条比例
                       @BeanProperty var appSalePercent: Integer,
                       @BeanProperty var eventName:      String,  //eventName

                       @BeanProperty var eventValue:     Integer, //eventValue
                       @BeanProperty var refer:          String,  //refer
                       @BeanProperty var status:         Integer, //下发状态
                       @BeanProperty var region:         String,
                       @BeanProperty var city:           String,

                       @BeanProperty var uid:            String,  //6/14
                       @BeanProperty var times:          Integer,
                       @BeanProperty var time:           Integer,
                       @BeanProperty var isNew:          Integer,
                       @BeanProperty var pbResp:         String, //postback response信息

                       @BeanProperty var recommender:    Integer,//推荐框架的算法标记
                       @BeanProperty var raterId:        String, //推荐框架的算法标记
                       @BeanProperty var raterType:      Integer,//推荐框架的算法标记
                       @BeanProperty var appName:        String, //2019.12.6新增
                       @BeanProperty var crId:           String, //2019.12.6新增
                       @BeanProperty var caId:           String, //2019.12.6新增
                       @BeanProperty var deviceid:       String, //2019.12.6新增

                       @BeanProperty var repeated:       String,
                       @BeanProperty var l_time:         String,
                       @BeanProperty var b_date:         String,
                       @BeanProperty var b_time:         String
                      ) extends PhoenixStorable {

  override def toSparkRow (): Row = {
    Row(repeats, rowkey,
        id,           publisherId, subId,       offerId,        campaignId,
        countryId,    carrierId,   deviceType,  userAgent,      ipAddr,
        clickId,      price,       reportTime,  createTime,     clickTime,
        showTime,     requestType, priceMethod, bidPrice,       adType,
        isSend,       reportPrice, sendPrice,   s1,             s2,
        gaid,         androidId,   idfa,        postBack,       sendStatus,
        sendTime,     sv,          imei,        imsi,           imageId,
        affSub,       s3,          s4,          s5,
        packageName,  domain,      respStatus,  winPrice,       winTime,
        appPrice,     test,        ruleId,      smartId,        reportIp,
        pricePercent, appPercent,  salePercent, appSalePercent, eventName,
        eventValue,   refer,       status,      region,         city,
        uid,          times,       time,        isNew,          pbResp,
        recommender,  raterId,     raterType,   appName,        crId,
        caId,         deviceid,
        repeated,  l_time, b_date, b_time)
  }

  override def structType: StructType = SspTrafficDWISchema.structType

  override def assembleFields (row: Array[Byte], source: collection.Map[(String, String), Array[Byte]]): Unit = {
//    val v = parseRowkey(
//      row,
//      classOf[String]
//    )

    repeats = ifNull(source.get(("0","repeats")), null,  Bytes.toInt(source.get(("0","repeats")).get) + Int.MaxValue + 1)
    rowkey = ifNull(source.get(("0","rowkey")), null, Bytes.toString(source.get(("0","rowkey")).get))

    id =  ifNull(source.get(("0","id")), null,  Bytes.toInt(source.get(("0","id")).get) + Int.MaxValue + 1)
    publisherId = ifNull(source.get(("0","publisherId")), null,  Bytes.toInt(source.get(("0","publisherId")).get) + Int.MaxValue + 1)
    subId =       ifNull(source.get(("0","subId")), null,  Bytes.toInt(source.get(("0","subId")).get) + Int.MaxValue + 1)
    offerId =     ifNull(source.get(("0","offerId")), null,  Bytes.toInt(source.get(("0","offerId")).get) + Int.MaxValue + 1)
    campaignId = ifNull(source.get(("0","campaignId")), null,  Bytes.toInt(source.get(("0","campaignId")).get) + Int.MaxValue + 1)

    countryId = ifNull(source.get(("0","countryId")), null,  Bytes.toInt(source.get(("0","countryId")).get) + Int.MaxValue + 1)
    carrierId = ifNull(source.get(("0","carrierId")), null,  Bytes.toInt(source.get(("0","carrierId")).get) + Int.MaxValue + 1)
    deviceType = ifNull(source.get(("0","deviceType")), null,  Bytes.toInt(source.get(("0","deviceType")).get) + Int.MaxValue + 1)
    userAgent = ifNull(source.get(("0","userAgent")), null,  Bytes.toString(source.get(("0","userAgent")).get))
    ipAddr = ifNull(source.get(("0","ipAddr")), null,  Bytes.toString(source.get(("0","ipAddr")).get))

    clickId = ifNull(source.get(("0","clickId")), null,  Bytes.toString(source.get(("0","clickId")).get))
    price = ifNull(source.get(("0","price")), null,  java.lang.Double.valueOf( Bytes.toString(source.get(("0","price")).get)))
    reportTime = ifNull(source.get(("0","reportTime")), null,  Bytes.toString(source.get(("0","reportTime")).get))
    createTime = ifNull(source.get(("0","createTime")), null,  Bytes.toString(source.get(("0","createTime")).get))
    clickTime = ifNull(source.get(("0","clickTime")), null, Bytes.toString(source.get(("0","clickTime")).get))

    showTime = ifNull(source.get(("0","showTime")), null, Bytes.toString(source.get(("0","showTime")).get))
    requestType = ifNull(source.get(("0","requestType")), null, Bytes.toString(source.get(("0","requestType")).get))
    priceMethod = ifNull(source.get(("0","priceMethod")), null, Bytes.toInt(source.get(("0","priceMethod")).get) + Int.MaxValue + 1)
    bidPrice = ifNull(source.get(("0","bidPrice")), null, java.lang.Double.valueOf( Bytes.toString(source.get(("0","bidPrice")).get)))
    adType = ifNull(source.get(("0","adType")), null, Bytes.toInt(source.get(("0","adType")).get) + Int.MaxValue + 1)

    isSend = ifNull(source.get(("0","isSend")), null,  Bytes.toInt(source.get(("0","isSend")).get) + Int.MaxValue + 1)
    reportPrice = ifNull(source.get(("0","reportPrice")), null,  java.lang.Double.valueOf( Bytes.toString(source.get(("0","reportPrice")).get)))
    sendPrice = ifNull(source.get(("0","sendPrice")), null, java.lang.Double.valueOf( Bytes.toString(source.get(("0","sendPrice")).get)))
    s1 = ifNull(source.get(("0","s1")), null,  Bytes.toString(source.get(("0","s1")).get))
    s2 = ifNull(source.get(("0","s2")), null,  Bytes.toString(source.get(("0","s2")).get))

    gaid = ifNull(source.get(("0","gaid")), null,  Bytes.toString(source.get(("0","gaid")).get))
    androidId = ifNull(source.get(("0","androidId")), null, Bytes.toString(source.get(("0","androidId")).get))
    idfa = ifNull(source.get(("0","idfa")), null,  Bytes.toString(source.get(("0","idfa")).get))
    postBack = ifNull(source.get(("0","postBack")), null,  Bytes.toString(source.get(("0","postBack")).get))
    sendStatus = ifNull(source.get(("0","sendStatus")), null,  Bytes.toInt(source.get(("0","sendStatus")).get) + Int.MaxValue + 1)

    sendTime = ifNull(source.get(("0","sendTime")), null,  Bytes.toString(source.get(("0","sendTime")).get))
    sv = ifNull(source.get(("0","sv")), null,  Bytes.toString(source.get(("0","sv")).get))
    imei = ifNull(source.get(("0","imei")), null,  Bytes.toString(source.get(("0","imei")).get))
    imsi = ifNull(source.get(("0","imsi")), null,  Bytes.toString(source.get(("0","imsi")).get))
    imageId = ifNull(source.get(("0","imageId")), null,  Bytes.toInt(source.get(("0","imageId")).get) + Int.MaxValue + 1)

    affSub = ifNull(source.get(("0","affSub")), null,  Bytes.toString(source.get(("0","affSub")).get))
    s3 = ifNull(source.get(("0","s3")), null,  Bytes.toString(source.get(("0","s3")).get))
    s4 = ifNull(source.get(("0","s4")), null,  Bytes.toString(source.get(("0","s4")).get))
    s5 = ifNull(source.get(("0","s5")), null,  Bytes.toString(source.get(("0","s5")).get))

    //新版写法
    packageName = getStr(source, "packageName")
    domain      = getStr(source, "domain")
    respStatus  = getInt(source, "respStatus")
    winPrice    = getDou(source, "winPrice")
    winTime     = getStr(source, "winTime")

    appPrice    = getDou(source, "appPrice")
    test        = getInt(source, "test")
    ruleId      = getInt(source, "ruleId")
    smartId     = getInt(source, "smartId")
    reportIp    = getStr(source, "reportIp")

    pricePercent   = getInt(source, "pricePercent")
    appPercent     = getInt(source, "appPercent")
    salePercent    = getInt(source, "salePercent")
    appSalePercent = getInt(source, "appSalePercent")

    eventName      = getStr(source, "eventName")
    eventValue     = getInt(source, "eventValue")
    refer          = getStr(source, "refer")
    status         = getInt(source, "status")
    region         = getStr(source, "region")
    city           = getStr(source, "city")

    uid            = getStr(source, "uid")
    times          = getInt(source, "times")
    time           = getInt(source, "time")
    isNew          = getInt(source, "isNew")
    pbResp         = getStr(source, "pbResp")

    recommender    = getInt(source, "recommender")
    raterId        = getStr(source, "raterId")
    raterType       = getInt(source, "raterType")

    appName       = getStr(source, "appName")
    crId          = getStr(source, "crId")
    caId          = getStr(source, "caId")
    deviceid      = getStr(source, "deviceid")

    repeated = ifNull(source.get(("0","repeated")), null,  Bytes.toString(source.get(("0","repeated")).get))
    l_time = ifNull(source.get(("0","l_time")), null,   Bytes.toString(source.get(("0","l_time")).get))
    b_date = ifNull(source.get(("0","b_date")), null,   Bytes.toString(source.get(("0","b_date")).get))
    b_time = ifNull(source.get(("0","b_time")), null,   Bytes.toString(source.get(("0","b_time")).get))
  }

  //保存
  override def toHBaseRowkey: Array[Byte] = {
    if(clickId == null ) null else Bytes.toBytes(clickId)
  }

  override def hashCode(): Int = {
    String.valueOf(clickId).hashCode
  }

  override def equals(obj: scala.Any): Boolean = {
    String.valueOf(clickId).equals(String.valueOf(obj))
  }

  override def toColumns: Map[(String, String), Array[Byte]] = {
    Map(

      ("0","repeats") ->    ifNull(repeats, null, Bytes.toBytes(repeats - Int.MaxValue - 1)),
      ("0","rowkey") ->     ifNull(rowkey, null, Bytes.toBytes(rowkey)),

      ("0","id") ->         ifNull(id, null, Bytes.toBytes(id - Int.MaxValue - 1)),
      ("0","publisherId") ->ifNull(publisherId, null, Bytes.toBytes(publisherId - Int.MaxValue - 1)),
      ("0","subId") ->      ifNull(subId, null, Bytes.toBytes(subId - Int.MaxValue - 1)),
      ("0","offerId") ->    ifNull(offerId, null, Bytes.toBytes(offerId - Int.MaxValue - 1)),
      ("0","campaignId") -> ifNull(campaignId, null, Bytes.toBytes(campaignId - Int.MaxValue - 1)),

      ("0","countryId") ->  ifNull(countryId, null, Bytes.toBytes(countryId - Int.MaxValue - 1)),
      ("0","carrierId") ->  ifNull(carrierId, null, Bytes.toBytes(carrierId - Int.MaxValue - 1)),
      ("0","deviceType") -> ifNull(deviceType, null, Bytes.toBytes(deviceType - Int.MaxValue - 1)),
      ("0","userAgent") ->  ifNull(userAgent, null, Bytes.toBytes(userAgent)),
      ("0","ipAddr") ->     ifNull(ipAddr, null, Bytes.toBytes(ipAddr)),

      ("0","clickId") ->    ifNull(clickId, null, Bytes.toBytes(clickId)),
      ("0","price") ->      ifNull(price, null, Bytes.toBytes(price + "")),
      ("0","reportTime") -> ifNull(reportTime, null, Bytes.toBytes(reportTime)),
      ("0","createTime") -> ifNull(createTime, null, Bytes.toBytes(createTime)),
      ("0","clickTime") ->  ifNull(clickTime, null, Bytes.toBytes(clickTime)),

      ("0","showTime") ->    ifNull(showTime, null, Bytes.toBytes(showTime)),
      ("0","requestType") -> ifNull(requestType, null, Bytes.toBytes(requestType)),
      ("0","priceMethod") -> ifNull(priceMethod, null, Bytes.toBytes(priceMethod - Int.MaxValue - 1)),
      ("0","bidPrice") ->    ifNull(bidPrice, null, Bytes.toBytes(bidPrice + "")),
      ("0","adType") ->      ifNull(adType, null, Bytes.toBytes(adType - Int.MaxValue - 1)),

      ("0","isSend") ->      ifNull(isSend, null, Bytes.toBytes(isSend - Int.MaxValue - 1)),
      ("0","reportPrice") -> ifNull(reportPrice, null, Bytes.toBytes(reportPrice + "")),
      ("0","sendPrice") ->   ifNull(sendPrice, null, Bytes.toBytes(sendPrice + "")),
      ("0","s1") ->          ifNull(s1, null, Bytes.toBytes(s1)),
      ("0","s2") ->          ifNull(s2, null, Bytes.toBytes(s2)),

      ("0","gaid") ->       ifNull(gaid, null, Bytes.toBytes(gaid)),
      ("0","androidId") ->  ifNull(androidId, null, Bytes.toBytes(androidId)),
      ("0","idfa") ->       ifNull(idfa, null, Bytes.toBytes(idfa)),
      ("0","postBack") ->   ifNull(postBack, null, Bytes.toBytes(postBack)),
      ("0","sendStatus") -> ifNull(sendStatus, null, Bytes.toBytes(sendStatus - Int.MaxValue - 1)),

      ("0","sendTime") -> ifNull(sendTime, null, Bytes.toBytes(sendTime)),
      ("0","sv") ->       ifNull(sv, null, Bytes.toBytes(sv)),
      ("0","imei") ->     ifNull(imei, null, Bytes.toBytes(imei)),
      ("0","imsi") ->     ifNull(imsi, null, Bytes.toBytes(imsi)),
      ("0","imageId") ->  ifNull(imageId, null, Bytes.toBytes(imageId - Int.MaxValue - 1)),

      ("0","affSub") ->   ifNull(affSub, null, Bytes.toBytes(affSub)),
      ("0","s3") ->       ifNull(s3, null, Bytes.toBytes(s3)),
      ("0","s4") ->       ifNull(s4, null, Bytes.toBytes(s4)),
      ("0","s5") ->       ifNull(s5, null, Bytes.toBytes(s5)),

      //新版写法
      ("0","packageName") -> setStr(packageName),
      ("0","domain")      -> setStr(domain),
      ("0","respStatus")  -> setInt(respStatus),
      ("0","winPrice")    -> setDou(winPrice),
      ("0","winTime")     -> setStr(winTime),

      ("0","appPrice")    -> setDou(appPrice),
      ("0","test")        -> setInt(test),
      ("0","ruleId")      -> setInt(ruleId),
      ("0","smartId")     -> setInt(smartId),
      ("0","reportIp")    -> setStr(reportIp),

      ("0","pricePercent")   -> setInt(pricePercent),
      ("0","appPercent")     -> setInt(appPercent),
      ("0","salePercent")    -> setInt(salePercent),
      ("0","appSalePercent") -> setInt(appSalePercent),

      ("0","eventName")      -> setStr(eventName),
      ("0","eventValue")     -> setInt(eventValue),
      ("0","refer")          -> setStr(refer),
      ("0","status")         -> setInt(status),
      ("0","region")         -> setStr(region),
      ("0","city")           -> setStr(city),

      ("0","uid")            -> setStr(uid),
      ("0","times")          -> setInt(times),
      ("0","time")           -> setInt(time),
      ("0","isNew")          -> setInt(isNew),
      ("0","pbResp")         -> setStr(pbResp),

      ("0","recommender")    -> setInt(recommender),
      ("0","raterId")        -> setStr(raterId),
      ("0","raterType")      -> setInt(raterType),

      ("0","appName")       -> setStr(appName),
      ("0","crId")          -> setStr(crId),
      ("0","caId")          -> setStr(caId),
      ("0","deviceid")      -> setStr(deviceid),

      ("0","repeated") -> ifNull(repeated, null, Bytes.toBytes(repeated)),
      ("0","l_time") ->   ifNull(l_time, null, Bytes.toBytes(l_time)),
      ("0","b_date") ->   ifNull(b_date, null, Bytes.toBytes(b_date)),
      ("0","b_time") ->   ifNull(b_time, null, Bytes.toBytes(b_time))
    )
  }


  def this () {
    this(
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],

      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],

      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[String],

      null.asInstanceOf[String],
      null.asInstanceOf[String],
      null.asInstanceOf[Integer],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[Integer],

      null.asInstanceOf[Integer],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[String],
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

      null.asInstanceOf[String],           //包名
      null.asInstanceOf[String],           //域名
      null.asInstanceOf[Integer],          //未下发原因
      null.asInstanceOf[java.lang.Double], //中签价格
      null.asInstanceOf[String],           //中签时间

      null.asInstanceOf[java.lang.Double], //app价格 Dsp V1.0.1 新增
      null.asInstanceOf[Integer],          //Dsp A/B测试 Dsp V1.0.1 新增
      null.asInstanceOf[Integer],          //smartLink + 新增
      null.asInstanceOf[Integer],
      null.asInstanceOf[String],

      null.asInstanceOf[Integer],  //单价比例
      null.asInstanceOf[Integer],  //app比例
      null.asInstanceOf[Integer],  //按条比例
      null.asInstanceOf[Integer],

      null.asInstanceOf[String],  //eventname
      null.asInstanceOf[Integer], //eventvalue
      null.asInstanceOf[String],  //refer
      null.asInstanceOf[Integer], //status
      null.asInstanceOf[String],  //region
      null.asInstanceOf[String],  //city

      null.asInstanceOf[String],  //uid
      null.asInstanceOf[Integer], //times
      null.asInstanceOf[Integer], //time
      null.asInstanceOf[Integer], //isNew
      null.asInstanceOf[String], //pbResp

      null.asInstanceOf[Integer], //recommender
      null.asInstanceOf[String], //raterId
      null.asInstanceOf[Integer], //raterType
      null.asInstanceOf[String], //2019.12.6新增
      null.asInstanceOf[String], //2019.12.6新增
      null.asInstanceOf[String], //2019.12.6新增
      null.asInstanceOf[String], //2019.12.6新增

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


