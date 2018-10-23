package com.mobikok.ssp.data.streaming.test

import java.sql.ResultSet

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util.{MySqlJDBCClientV2, OM}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/1/17.
  */

object OMTest{

  def main (args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //    .set("spark.sql.caseSensitive","false")
      .setAppName("wordcount")
      .setMaster("local[*]")

    println("xxxxxxxxxxxxxx2")
    val sc = new SparkContext(sparkConf)

    println("xxxxxxxxxxxxxx3")
    val sq = new SQLContext(sc)
    println("xxxxxxxxxxxxxx4")

    import sq.implicits._

    Seq("asd").toDF("S").rdd.map{x=>
      println("exe...................")
    }.collect()
    println("ccccccccccccccc")


  }
  def main3 (args: Array[String]): Unit = {
    var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
      "",
      "jdbc:mysql://192.168.1.26:3306/kok_adv_bd?autoReconnect=true&useUnicode=true&characterEncoding=utf8",
      "root",
      "1234")

    mySqlJDBCClientV2.executeQuery("select * from mysql.user", new Callback[Object] {
      override def onCallback (rs: ResultSet): Object = {
        println(OM.assembleAs(rs, classOf[Object]))
        ""
      }
    });


  }
  def main2 (args: Array[String]): Unit = {
    var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
      "",
      "jdbc:mysql://node17:3306/sight",
      "root",
      "root_root")

    mySqlJDBCClientV2.executeQuery("select * from sight.offset", new Callback[String] {
      override def onCallback (rs: ResultSet): String = {

        println("xxxxxxxxxxxxxx")
        var sparkConf = new SparkConf()
          .set("hive.exec.dynamic.partition", "true")
          .set("hive.exec.dynamic.partition.mode", "nonstrict")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          //    .set("spark.sql.caseSensitive","false")
          .setAppName("wordcount")
          .setMaster("local[*]")

        println("xxxxxxxxxxxxxx2")
        val sc = new SparkContext(sparkConf)

        println("xxxxxxxxxxxxxx3")
        val sq = new SQLContext(sc)
        println("xxxxxxxxxxxxxx4")
        OM.assembleAsDataFrame(rs, sq).show(10)


        ""
      }
    })
  }
}