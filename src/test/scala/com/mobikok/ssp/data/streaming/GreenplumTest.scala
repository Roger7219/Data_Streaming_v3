package com.mobikok.ssp.data.streaming

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.mobikok.ssp.data.streaming.client.TransactionManager
import com.pivotal.jdbc.GreenplumDriver
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.ByteType

/**
  * Created by Administrator on 2017/9/22.
  */
object GreenplumTest {

  var config: Config = ConfigFactory.load
  val transactionManager = new TransactionManager(config)
  var sparkConf:SparkConf = null /*new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")*/

  val sc:SparkContext = null // new SparkContext(sparkConf)

  val hiveContext:HiveContext = null// new HiveContext(sc)

  Class.forName("org.postgresql.Driver")
  private val options = Map(
    "url" -> "jdbc:postgresql://node15:5432/postgres?reWriteBatchedInserts=true&autosave=CONSERVATIVE",
    "user" -> "gpadmin",
    "password" -> "gpadmin",
    "driver" -> "org.postgresql.Driver"
  )

  def main (args: Array[String]): Unit = {


    hive2gp

  }

  def hive2gp(): Unit ={

    ByteType
    import  org.apache.spark.sql.functions._
    val s=hiveContext
      .read
      .table("ADVERTISER2")
      .select(col("*"), expr("'-' as exchange_partition"))
      s.printSchema()
      s.write
      .mode(SaveMode.Append).format("jdbc")
      .options(options).option("dbtable", "advertiser_default_partition_tmp").save()
  }
  def selec2() ={

    Class.forName(classOf[GreenplumDriver].getName);
    //
    //    val db =   DriverManager.getConnection("jdbc:postgresql://node15:5432/postgres?reWriteBatchedInserts=true&autosave=CONSERVATIVE",  "gpadmin", "gpadmin")
    val db = DriverManager.getConnection("jdbc:pivotal:greenplum://node15:5432/postgres?reWriteBatchedInserts=11;autosave=CONSERVATIVE;DatabaseName=postgres", "gpadmin", "gpadmin");
    val st = db.createStatement();
    val rs = st.executeQuery("SELECT * FROM t_hash2 limit 11");
    var i =0
    while (i < 10  && rs.next()) {
      System.out.println(rs.getString(1));
      i+=1
      print("--------")
    }
    print("end")
    rs.close();
    st.close();
    db.close();
  }
  def select (): Unit ={
    Class.forName(classOf[GreenplumDriver].getName);
    val db = DriverManager.getConnection("jdbc:pivotal:greenplum://node15:5432;DatabaseName=postgres", "gpadmin", "gpadmin");
    val st = db.createStatement();
    val rs = st.executeQuery("SELECT datname FROM pg_database");
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }
    rs.close();
    st.close();
  }



}
