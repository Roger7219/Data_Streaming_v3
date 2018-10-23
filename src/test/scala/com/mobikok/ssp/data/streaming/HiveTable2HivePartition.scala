package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.Mysql2HiveTest.getDate
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2017/10/9.
  */
object HiveTable2HivePartition {

//  val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp_stat?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
  val rdbUrl = "jdbc:mysql://104.250.132.218:8905/kok_ssp_stat?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
  val rdbUser = "root"
  val rdbPassword = "@dfei$@DCcsYG"

  val tablePrefix = "DATA_STAT"
//  var hiveTableList = Array(s"${tablePrefix}_201705",s"${tablePrefix}_201706",s"${tablePrefix}_201707",s"${tablePrefix}_201708",s"${tablePrefix}_201709")
  var hiveTableList = Array(s"${tablePrefix}_201705")

  var hiveTable = tablePrefix

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

//      .set("spark.default.parallelism", "400")
//      .set("spark.sql.shuffle.partitions", "400")

//      .setAppName("Mysql2HiveTest")
//      .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)

    //get StatData from mysqlTable
    hiveTableList.foreach { table =>
     val statdateList = hiveContext
        .read
//        .jdbc(rdbUrl, table, rdbProp)
        .table(table)
        .selectExpr("StatDate")
        .distinct()
        .sort("StatDate")
        .collect()

      statdateList.foreach{
        row =>
        //write data to hive partition from hive table
//        println(getDate(row.toString()))
          val t2 = hiveContext
              .read
              .table(table)

          val f1=t2.schema.fieldNames(0)
          val fs= t2.schema.fieldNames.tail
          val f2=fs(0)
          val fn= fs.tail :+ f1

          t2.where(s"statdate = '${getDate(row.toString())}'")
          .coalesce(1)
          .select(f2, fn:_*)
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
