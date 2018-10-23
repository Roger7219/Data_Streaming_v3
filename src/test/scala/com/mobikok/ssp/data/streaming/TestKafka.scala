package com.mobikok.ssp.data.streaming

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{Logger, LoggerFactory}

class X22{
  def this(s:String){
    this()
    println(s)
  }
}
/**
  * Created by admin on 2017/10/31.
  */
object TestKafka {

  def main(args: Array[String]): Unit = {
//    val logger: Logger = LoggerFactory.getLogger(classOf[KafkaSender])
//    val sparkConf = new SparkConf().set("hive.exec.dynamic.partition.mode", "nonstrict").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////          .setAppName("TestKafka")
////          .setMaster("local[4]")
//
//    val sc = new JavaSparkContext(sparkConf)
//    val hc = new HiveContext(sc)
////    val ht = "ssp_send_dwi"
//    val ht = "ssp_send_dwi"
//    		hc
//        .read
//        .table(ht)
//        .where("b_date = \"2017-10-30\"")
//        .limit(1000000)
//        .toJSON
//        .foreach{ row =>
////          logger.warn(s"messg of send to kafka is :$row")
//          KafkaSender.send("kafka_test_topic_1102", row)
//        }
//
//    Thread.sleep(1000 * 60 * 1)
//    sc.stop()
  }

}
