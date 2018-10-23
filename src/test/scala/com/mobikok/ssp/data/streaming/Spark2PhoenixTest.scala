package com.mobikok.ssp.data.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SQLContext, SaveMode}

import  org.apache.spark.sql.functions._
import scala.reflect.internal.util.TableDef.Column

/**
  * Created by Administrator on 2017/6/20.
  */
object Spark2PhoenixTest {

  val sc:SparkContext = null//new SparkContext("local", "phoenix-test")
  val sqlContext:SQLContext = null ///new SQLContext(sc)

  def main (args: Array[String]): Unit = {
//    save
    load
  }

  def save ={
    val dataSet = List((111L, 2, 1, 1,222, "2014-12-12"), (1L, 3, 1, 1,333, "2015-12-12"),(1L, 2, 1, 1,4444, "2016-12-12"))

    import org.apache.phoenix.spark._
//方法1 rdd save
    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "ssp_user_dm_hbase",
        Seq("appid","countryid","carrierid", "sdkversion","times", "b_date"),
        //zkUrl = Some("node17:2181")
        new Configuration()
      )


//方法2 rdd save
//    import org.apache.phoenix.spark._
//
//    val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
//      "TABLE1", Seq("ID", "COL1"), zkUrl = Some("phoenix-server:2181")
//    )

//方法3 rdd save
// Save to OUTPUT_TABLE
//    df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE",
//      "zkUrl" -> hbaseConnectionString))

  }

  def load = {
//方法1 df load
//    import org.apache.phoenix.spark._
//    val df = sqlContext.phoenixTableAsDataFrame(
//      "ssp_user_dm_hbase", Array("appid" ), conf = new Configuration()
//    )
//
//    df.show


//方法2 df load
    val df = sqlContext.load(
      "org.apache.phoenix.spark",
      Map(
        "table" -> "ssp_user_dm__hbase",
        "zkUrl" -> "node17:2181"
      )
    )
      //.where(col("appid")=== lit(1) and col("COUNTRYID") === 1   )

//
//        val df = sqlContext
//          .read
//          .format("org.apache.phoenix.spark")
//          .options(Map("table" -> "ssp_user_dm__hbase", "zkUrl" -> "node17:2181"))
//          .load
//          .filter("times = 1")


    import org.apache.spark.sql.functions._
    val newdf = df.alias("a")
      .union(df.alias("b"))
      .groupBy("appid", "countryid", "carrierid", "sdkversion", "times", "b_date")
      .agg(sum("times"))
    //.join(df.alias("b"), col("a.appid") === col("b.appid"), "left_outer" )
    newdf
      //.filter(df("appid") === 1)
      //      .select(df("appid"))
      .show
  }


}
