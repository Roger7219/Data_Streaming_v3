package com.mobikok.ssp.data.streaming

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by admin on 2017/10/9.
  */
object Mysql2HiveTest {

//  val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp_stat?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
  val rdbUrl = "jdbc:mysql://104.250.132.218:8905/kok_ssp_stat?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
  val rdbUser = "root"
  val rdbPassword = "@dfei$@DCcsYG"
//  val tablePrefix = "ADVER_STAT"
//  var mysqlTableList = Array(s"${tablePrefix}_201612",s"${tablePrefix}_201701",s"${tablePrefix}_201702"
//                            ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
//                            ,s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
//                            ,s"${tablePrefix}_201709")

  val tablePrefix = "DATA_STAT"
//  var mysqlTableList = Array(s"${tablePrefix}_201609",s"${tablePrefix}_201610",s"${tablePrefix}_201612")
//  var mysqlTableList = Array(s"${tablePrefix}_201611")
  var mysqlTableList = Array(/*s"${tablePrefix}_201701",s"${tablePrefix}_201702"
    ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
    ,*/s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
    ,s"${tablePrefix}_201709")


//  val tablePrefix = "DATA_FILL_STAT"
  ////    var mysqlTableList = Array(s"${tablePrefix}_201609",s"${tablePrefix}_201610")
  //    var mysqlTableList = Array(s"${tablePrefix}_201611",s"${tablePrefix}_201612")
  ////  var mysqlTableList = Array(s"${tablePrefix}_201701",s"${tablePrefix}_201702"
  ////    ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
  ////    ,s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
  ////    ,s"${tablePrefix}_201709")

//  val tablePrefix = "USER_STAT"
//  var mysqlTableList = Array(s"${tablePrefix}_201610",s"${tablePrefix}_201611",s"${tablePrefix}_201612",s"${tablePrefix}_201701",s"${tablePrefix}_201702"
//      ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
//      ,s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
//      ,s"${tablePrefix}_201709")

//  val tablePrefix = "USER_RETENTION"
////  var mysqlTableList = Array(s"${tablePrefix}_201610")
//  var mysqlTableList = Array(s"${tablePrefix}_201611",s"${tablePrefix}_201612",s"${tablePrefix}_201701",s"${tablePrefix}_201702"
//    ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
//    ,s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
//    ,s"${tablePrefix}_201709")

//  val tablePrefix = "LOG_STAT"
//  var mysqlTableList = Array(s"${tablePrefix}_201704",s"${tablePrefix}_201705"
//    ,s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708"
//    ,s"${tablePrefix}_201709")

//  val tablePrefix = "TOP_OFFER"
//  var mysqlTableList = Array(s"${tablePrefix}_201611",s"${tablePrefix}_201612",s"${tablePrefix}_201701",s"${tablePrefix}_201702"
//      ,s"${tablePrefix}_201703",s"${tablePrefix}_201704",s"${tablePrefix}_201705"
//      ,s"${tablePrefix}_201706",s"${tablePrefix}_201707")
//var mysqlTableList = Array(s"${tablePrefix}_201708",s"${tablePrefix}_201709")

//  val tablePrefix = "TOP_FLOW"
//  var mysqlTableList = Array(s"${tablePrefix}")


  var hiveTable = tablePrefix
  val rdbProp = new java.util.Properties {
    {
      setProperty("user", rdbUser)
      setProperty("password", rdbPassword)
      setProperty("driver", "com.mysql.jdbc.Driver")
    }
  }

  def getDate(row :String) :String= {
    val s = row.replace("[","")
    s.replace("]","")
  }

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.shuffle.blockTransferService", "nio")

      .set("spark.default.parallelism", "400")
      .set("spark.sql.shuffle.partitions", "400")

//      .setAppName("Mysql2HiveTest")
//      .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)

    //get StatData from mysqlTable
    mysqlTableList.foreach { table =>
      val statdateList = hiveContext
        .read
        .jdbc(rdbUrl, table, rdbProp)
        .selectExpr("StatDate")
        .distinct()
        .sort("StatDate")
        .collect()

      statdateList.foreach{
        row =>
        //write data to hive table from mysql

          val t2 = hiveContext
              .read
              .jdbc(rdbUrl, table, rdbProp)

          val f1=t2.schema.fieldNames(0)
          val fs= t2.schema.fieldNames.tail
          val f2=fs(0)
          val fn= fs.tail :+ f1

//          val fn = fs.tail.filter( _ != "RequestCount)") :+ "SendFeeCount" :+ "RequestCount" :+ "ThirdFee" :+ "ThirdSendFee" :+ "ThirdClickCount") :+ f1
          t2.where(s"statDate = '${getDate(row.toString())}'")
          .coalesce(1)
          .select(f2, fn:_*)
//          .selectExpr("PublisherId", "AppId", "CountryId","CarrierId", "SdkVersion", "0 as AdType", "TotalCount", "SendCount","StatDate")
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(hiveTable)
      }

    }
    val stopTime = System.currentTimeMillis()
    println("Mysql2HiveTest takes " + (stopTime - startTime)/1000 + "second")
    sc.stop()
  }
}
