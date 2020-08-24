package com.mobikok.ssp.data.streaming.client

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientTest {

  var config: Config = ConfigFactory.load
  var sparkConf:SparkConf = null/*new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")*/

  val sc:SparkContext = null// new SparkContext(sparkConf)
  val hiveContext:HiveContext = null ///new HiveContext(sc)
  val hbaseClient:HBaseClient = null// new HBaseClient("test_moudle", sc, config, transactionManager, new ModuleTracer())
//  hbaseClient.init()

  def main (args: Array[String]): Unit = {
    getsAsRDD()
  }


  def  getsAsRDD(): Unit ={

//    val dwi = AggTrafficDWI(
//      1, "",
//      1,1,Array(2,3),1,"imei1",
//      "imsi1","version1","model1","screen1",1,
//      "sv1","leftSize1","androidId1","userAgent1",1,
//      "createTime1","clickTime1","showTime1","reportTime1", 1,
//      1,"ipAddr1",1,"pkgName1","s11",
//      "s21","clickId1",1.1,1,"affSub1",
//      "repeated1","l_time1","b_date1"
//    )
//    hbaseClient.putsNonTransaction("SSP_AGG_FILL_DWI_PHOENIX", Array(dwi))
    val keys = sc.parallelize(List("clickId1") )

//    val v = hbaseClient.getsAsDF("SSP_AGG_FILL_DWI_PHOENIX", keys, classOf[AggTrafficDWI])
//    v.schema.printTreeString()
//    v.foreach{println(_)}

//    val v = hbaseClient.getsAsRDD("SSP_AGG_FILL_DWI_PHOENIX", keys, classOf[SspAggTrafficDWI])//.foreach{println(_)}
//    val v1 = hiveContext.createDataFrame(v)
//      v1.printSchema()
//     v1 .foreach(println(_))
  }


}
