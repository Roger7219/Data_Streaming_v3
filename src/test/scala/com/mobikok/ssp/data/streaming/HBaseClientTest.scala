package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.HBaseClientPutTest.sparkConf
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.client.{HBaseClient, TransactionManager}
import com.mobikok.ssp.data.streaming.util.ModuleTracer
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
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientTest {

  var config: Config = ConfigFactory.load
  val transactionManager = new TransactionManager(config)
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
