package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.client.{HBaseClient, TransactionManager}
import com.mobikok.ssp.data.streaming.entity.SspTrafficDWI
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.util.OM
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

/**
  * Created by Administrator on 2017/6/20.
  */
object OMTest  {

  var config: Config = ConfigFactory.load


  def main (args: Array[String]): Unit = {

    val s2="""{
      "repeats" : null,
      "rowkey" : null,
      "id" : null,
      "publisherId" : null,
      "subId" : null,
      "offerId" : null,
      "campaignId" : null,
      "countryId" : null,
      "carrierId" : null,
      "deviceType" : null,
      "userAgent" : null,
      "ipAddr" : null,
      "clickId" : null,
      "price" : 0.0,
      "reportTime" : null,
      "createTime" : null,
      "clickTime" : null,
      "showTime" : null,
      "requestType" : null,
      "priceMethod" : null,
      "bidPrice" : 0.0,
      "adType" : null,
      "isSend" : null,
      "reportPrice" : 0.0,
      "sendPrice" : 0.0,
      "s1" : null,
      "s2" : null,
      "gaid" : null,
      "androidId" : null,
      "idfa" : null,
      "postBack" : null,
      "sendStatus" : null,
      "sendTime" : null,
      "sv" : null,
      "imei" : null,
      "imsi" : null,
      "imageId" : 11,
      "repeated" : null,
      "l_time" : null,
      "b_date" : null,
      "hbaseRowkey" : 111
    }"""


    val ss= OM.toBean(s2, classOf[SspTrafficDWI])

    println(Array(ss).asInstanceOf[Array[_<:String]].getClass.getComponentType.getName)



  }

}
