package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.mobikok.ssp.data.streaming.entity.{SspTrafficDWI, UuidStat}
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.transaction.{AlwaysTransactionalStrategy, TransactionManager}
import com.mobikok.ssp.data.streaming.util.{CSTTime, ModuleTracer, OM}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import org.apache.spark
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientPutPhoenixDataTest  {

  var config: Config = ConfigFactory.load

  var sparkConf:SparkConf = null /*= new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstr4ict")
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local[2]")*/

  val sc:SparkContext = null //new SparkContext(sparkConf)

  val hbaseClient = new HBaseClient("test_moudle", sc, config, new TransactionManager(
    config,
    new AlwaysTransactionalStrategy(CSTTime.formatter("yyyy-MM-dd HH:00:00"), CSTTime.formatter("yyyy-MM-dd 00:00:00"))
  ), new ModuleTracer ())

  val hc = new HiveContext(sc)
  import hc.implicits._

  def main (args: Array[String]): Unit = {

    val schema = StructType(
      Seq(
        StructField("name",StringType,true)
        ,StructField("age",IntegerType,true)
      )
    )

    val  s= Seq(
      "123",
      "123").toDF("asd")

    val s2=""" {
             |  "repeats" : 0,
             |  "rowkey" : "",
             |  "id" : 0,
             |  "publisherId" : 184,
             |  "subId" : 860,
             |  "offerId" : 7484,
             |  "campaignId" : 285,
             |  "countryId" : 210,
             |  "carrierId" : 641,
             |  "deviceType" : 1,
             |  "userAgent" : "Mozilla/5.0(Linux;Android5.1;M12Build/LMY47I;wv)AppleWebKit/537.36(KHTML,likeGecko)Version/4.0Chrome/43.0.2357.121MobileSafari/537.36",
             |  "ipAddr" : "197.250.99.176",
             |  "clickId" : "4d0cb284-d4b3-4dce-bfb0-7cd482b330ac",
             |  "price" : 4.0E-4,
             |  "reportTime" : "",
             |  "createTime" : "2017-08-10 17:04:31",
             |  "clickTime" : "2017-08-10 17:04:32",
             |  "showTime" : "",
             |  "requestType" : "api",
             |  "priceMethod" : 3,
             |  "bidPrice" : 4.0E-4,
             |  "adType" : 4,
             |  "isSend" : 1,
             |  "reportPrice" : 0.08,
             |
             |  "sendPrice" : 0.0,
             |  "s1" : "",
             |  "s2" : "",
             |  "gaid" : "2e7e4477-9c98-41fe-997f-2824481cc055",
             |  "androidId" : "91d921e87cb08202",
             |
             |  "idfa" : "",
             |  "postBack" : "http://hw.vsoyou.net/inf/adsync/kok_ssp.htm?offid={appId}&tid={tid}&payout={payout}&ip={ip}&s1={s1}&s2={s2}",
             |  "sendStatus" : 0,
             |  "sendTime" : "",
             |  "sv" : "v1.3.0",
             |
             |  "imei" : "355962083152837",
             |  "imsi" : "640050641060516",
             |  "imageId" : 0,
             |  "repeated" : "N",
             |  "l_time" : "2017-08-10 20:00:00",
             |  "b_date" : "2017-08-10",
             |  "hbaseRowkey" : "NGQwY2IyODQtZDRiMy00ZGNlLWJmYjAtN2NkNDgyYjMzMGFjAA=="
             |}""".stripMargin


    val  cd = OM.toBean(s2, classOf[SspTrafficDWI])

    //
//    val  cd = new SspTrafficDWI()
//    cd.setClickId("clickid111")
//    cd.setAdType(11)
//    cd.setBidPrice(11.2)
//    cd.setIsSend(221)
//    cd.setPrice(11.22)
    val a = Array[SspTrafficDWI](cd)

    try {
      hbaseClient.putsNonTransaction("SSP_CLICK_DWI_PHOENIX2", a)

    }catch {
      case(ex: Exception) =>  ex.printStackTrace()
    }

  }

  private def scan0 (table: String,
                     startRow: Array[Byte],
                     stopRow: Array[Byte],
                     clazz: Class[_ <: HBaseStorable]
                    ): RDD[_ <: HBaseStorable] = {

    val scan = new Scan()
    if (startRow != null) scan.setStartRow(startRow)
    if (stopRow != null) scan.setStopRow(stopRow)

    val conf = createReadConf(table, scan)

    val rdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //HBaseStorable
    //    rdd.map { case (b, result) =>
    //      assemble(result, clazz)
    //    }

    rdd
      .map { x =>
        assemble(x._2, clazz)
      }
  }


  def createReadConf (table: String, scan: Scan): Configuration = {
    val p = ProtobufUtil.toScan(scan);
    val scanStr = Base64.encodeBytes(p.toByteArray());

    @transient val conf = HBaseConfiguration.create()
    config
      .getConfig("hbase.set")
      .entrySet()
      .foreach { x =>
        conf.set(x.getKey, x.getValue.unwrapped().toString)
      }
    // conf.set("hbase.zookeeper.property.clientPort", config.getConfig("hbase.set").getString("hbase.zookeeper.property.clientPort"))
    // conf.set("hbase.zookeeper.quorum", config.getConfig("hbase.set").getString("hbase.zookeeper.quorum"))
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set(TableInputFormat.SCAN, scanStr)
    conf
  }

  private def assemble (result: Result, clazz: Class[_ <: HBaseStorable]): HBaseStorable = {
    if(result == null || result.isEmpty) {
      return null
    }
    val h = clazz.newInstance()
    h//.setRowkey(result.getRow)

    var map = Map[(String, String), Array[Byte]]()
    result.getNoVersionMap.foreach { x =>
      x._2.foreach { y =>
        map += ((Bytes.toString(x._1) -> Bytes.toString(y._1)) -> y._2)
      }
    }
    h.assembleFields(result.getRow, map)
    h
  }



  private def createWriteConf (): Configuration = {
    val conf = HBaseConfiguration.create

    conf.set("hbase.zookeeper.quorum", "node14,node17,node15")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "ssp_user_dwi_uuid_stat_trans_20170621_003748_367__1")
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[org.apache.hadoop.hbase.io.ImmutableBytesWritable]])
    job.getConfiguration
  }


}
