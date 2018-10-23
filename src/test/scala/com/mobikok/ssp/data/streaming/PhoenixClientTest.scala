package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.client.{PhoenixClient, TransactionManager}
import com.mobikok.ssp.data.streaming.entity.{SspSendDM, SspUserNewAndActiveDM, SspUserNewAndActiveDMSchema}
import com.mobikok.ssp.data.streaming.util.{ModuleTracer, OM}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/23.
  */
object PhoenixClientTest {
  var config: Config = ConfigFactory.load
  val transactionManager = new TransactionManager(config)
  var sparkConf:SparkConf = null /*new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstr4ict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")*/


  val sc:SparkContext = null// new SparkContext(sparkConf)
  val hiveContext:HiveContext = null//new HiveContext(sc)

  val t = new TransactionManager(config)

  val pc = new PhoenixClient("test_module",sc, config, t, new ModuleTracer())
  pc.init()
  def main (args: Array[String]): Unit = {

    gets2()
//    gets()
//    upsertSum2()
  }

  def upsertSum2(): Unit ={
    import org.apache.spark.sql.functions._

    val rdd = sc.parallelize(Seq( Row( 1, 2, 3, 4,      "2012-12-12", 2L, "2017-06-20 12:12:12") ))
    val unionAggExprsWithAlias = List(expr("sum(TIMES)").as("TIMES"))

    pc.upsertUnionSum(
      "tid_1",
      "SSP_USER_DM_PHOENIX",
      classOf[SspUserNewAndActiveDM],
      hiveContext.createDataFrame(rdd, SspUserNewAndActiveDMSchema.structType),
      unionAggExprsWithAlias,
      Array("APPID", "COUNTRYID", "CARRIERID", "SDKVERSION","B_DATE"),
      "L_TIME"
    )

  }
  def upsertSum(): Unit ={
    import org.apache.spark.sql.functions._

    val rdd = sc.parallelize(Seq( Row("appid1", 222, "111") ))
    val unionAggExprsWithAlias = List(expr("cast( sum(cast(TIMES as Decimal(19,10))) as string)").as("TIMES"))

    pc.upsertUnionSum(
      "tid_1",
      "TEST27",
      classOf[Test27],
      hiveContext.createDataFrame(rdd, new Test27().structType),
      unionAggExprsWithAlias,
      Array("APPID", "APPID2")
    )
  }

  def gets(): Unit ={
    //    val x=Seq(1,1,1,1,"test_row_1")
    //    val xx= pc.gets("SSP_USER_DM__HBASE",Array(x), classOf[AdUserDm])
    //
//           val x=Seq("appid1","appid2")
//        val xx= pc.gets("TEST23",Array(x), classOf[Test23])
//    //
//    val x=Seq("appid1",222)
//    val xx= pc.gets("TEST24",Array(x), classOf[Test24])

       val x=Seq("appid1",222)
             val xx= pc.gets("TEST25",Array(x), classOf[Test25])

    println("============================================================")
    println(OM.toJOSN(xx))
    println("============================================================")
  }

  def gets2(): Unit ={
    //    val x=Seq(1,1,1,1,"test_row_1")
    //    val xx= pc.gets("SSP_USER_DM__HBASE",Array(x), classOf[AdUserDm])
    //
    //           val x=Seq("appid1","appid2")
    //        val xx= pc.gets("TEST23",Array(x), classOf[Test23])
    //    //
    //    val x=Seq("appid1",222)
    //    val xx= pc.gets("TEST24",Array(x), classOf[Test24])

    val x=Seq(141,17 ,43,2,"1.0.1",5,423,7932,"2017-06-21")
    val xx= pc.gets("SSP_SEND_DM_PHOENIX",Array(x), classOf[SspSendDM])

    println("============================================================")
    println(OM.toJOSN(xx))
    println("============================================================")
  }

}
