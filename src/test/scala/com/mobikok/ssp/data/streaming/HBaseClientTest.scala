package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientTest {

  var config: Config = ConfigFactory.load
  var sparkConf = null /* new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstr4ict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")*/

  val sc:SparkContext = null// new SparkContext(sparkConf)
  val hbaseClient:HBaseClient = null // new HBaseClient("test_moudle", sc, config, transactionManager, new ModuleTracer())
//  hbaseClient.init()

  def main (args: Array[String]): Unit = {

    hbaseClient.getsAsDF("ssp_fee_dwi_uuid_stat", Array("11"), classOf[UuidStat]).alias("saved")
    println("xxxxxxxxxxxxxxx")
  }


  def  test0(): Unit ={
    val a = Array[UuidStat](UuidStat("UUID_TEST44", 1))//.setRowkey(Bytes.toBytes("UUID_TEST44")))
    hbaseClient.init()
    hbaseClient.puts("test_tid_1", "ssp_user_dwi_uuid_stat_trans_20170621_013544_808__1", a)
    //
    //    hbaseClient.scanAsDF("ssp_user_dwi_uuid_stat_trans_20170621_013544_808__1", null, null, classOf[UuidStat], new  com.mobikok.ssp.data.streaming.schema.UuidStat().structType)
    //      .rdd
    //      .map{x=>
    //        println("======================" +x)
    //      }

    //    scan0("ssp_user_dwi_uuid_stat_trans_20170621_013544_808__1_trans_test_tid_1", null, null, classOf[UuidStat])
    //      .map { x =>
    //        println("======================" + x)
    //      }.collect()


    new HBaseScan().scan0("ssp_user_dwi_uuid_stat_trans_20170621_013544_808__1_trans_test_tid_1", null, null, classOf[UuidStat])
      .map { x =>
        println("======================" + x)
      }.collect()
  }


}
