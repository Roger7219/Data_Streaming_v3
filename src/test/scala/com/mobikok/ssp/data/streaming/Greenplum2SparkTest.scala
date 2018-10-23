package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.client.TransactionManager
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/20.
  */
object Greenplum2SparkTest {

  var config: Config = ConfigFactory.load
  val transactionManager = new TransactionManager(config)
  var sparkConf:SparkConf = null /*new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")*/

  val sc:SparkContext = null//new SparkContext(sparkConf)

  val hiveContext = new HiveContext(sc)

  def main (args: Array[String]): Unit = {
    println("-------------------")
//    load
//    insert
    insertParition
//    hiveContext.sql("insert into table dual select * from dual")
  }

  def insert :Unit ={

    //  spark-shell --jars /root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar

    Class.forName("org.postgresql.Driver")

    val options = Map(
      "url" -> "jdbc:postgresql://node15:5432/postgres", // JDBC url
      "user" -> "gpadmin",
      "password" -> "gpadmin",
//      "partitionColumn" ->"col_int",
      "driver" -> "org.postgresql.Driver",// JDBC driver
      "dbtable" -> "t_hash3") // Table name

    val sqlContext = new SQLContext(sc)
    val df_read_final = sqlContext.read.format("jdbc").options(options).load // Reads data as 1 partition
    df_read_final.printSchema()

    df_read_final.write.mode(SaveMode.Overwrite).format("jdbc").options(options).option("dbtable", "t_hash2").save()

  }

  def insertParition :Unit ={

    //  spark-shell --jars /root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar

    Class.forName("org.postgresql.Driver")

    val options = Map(
      "url" -> "jdbc:postgresql://node15:5432/postgres", // JDBC url
      "user" -> "gpadmin",
      "password" -> "gpadmin",
//      "partitionColumn" ->"date",
      "driver" -> "org.postgresql.Driver",// JDBC driver
      "dbtable" -> "sales") // Table name

    val sqlContext = new SQLContext(sc)
    val df_read_final0 = sqlContext.read.format("jdbc").options(options).load // Reads data as 1 partition
    df_read_final0.printSchema()

    import sqlContext.implicits._
    val df_read_final= Seq((2, "2013-01-01", 22.15)).toDF("id","date","amt") //.toDF("A2", "B","C")

    //：Table "sales4_1_prt_sales4_2013_01_011" is a child partition of table "sales4".  To drop it, use ALTER TABLE "sales4" DROP PARTITION "sales4_2013_01_01"...
    df_read_final.write.mode(SaveMode.Append).format("jdbc").options(options).option("dbtable", "sales4_1_prt_sales4_2013_01_011").save()
    // select * from sales4_1_prt_sales4_2013_01_011;

//
//    ALTER TABLE sales4 EXCHANGE PARTITION FOR ('2013-01-01')
//    WITH TABLE jan12;
  }



  def load :Unit ={

   //  spark-shell --jars /root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar

    Class.forName("org.postgresql.Driver")

    val options = Map(
      "url" -> "jdbc:postgresql://node15:5432/postgres", // JDBC url
      "user" -> "gpadmin",
      "password" -> "gpadmin",
      "driver" -> "org.postgresql.Driver",// JDBC driver
      "lowerBound"->"1",
      "upperBound"->"1000",
      "dbtable" -> "t_hash2") // Table name


    import org.apache.spark.sql.{SQLContext, SaveMode}

    val sqlContext = new SQLContext(sc)
    val df_read_final = sqlContext.read.format("jdbc").options(options).load // Reads data as 1 partition
    df_read_final.printSchema()
    println(df_read_final.limit(100).selectExpr("max(key)").show())

  }
}
