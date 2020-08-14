package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 2017/6/20.
  */
object Spark2HiveTest {

  import scala.collection.JavaConversions._

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  var LOG:com.mobikok.ssp.data.streaming.util.Logger  = new com.mobikok.ssp.data.streaming.util.Logger("Spark2HiveTest", this.getClass)


  var config: Config = ConfigFactory.load
  var sparkConf = new SparkConf()
    .set("hive.exec.dynamic.partition", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.sql.caseSensitive","false")
    .setAppName("wordcount")
    .setMaster("local[*]")
//    .setMaster("yarn-cluster")



  lazy val hiveContext:HiveContext = null //new HiveContext(sc)

  lazy val sqlContext:HiveContext = null // new HiveContext(sc)

  def main (args: Array[String]): Unit = {
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\pluggable\\新建文件夹 (11)")
    val sqlC= new SQLContext(sc)
    AS3.main3(sqlC)
  }


  def main2 (): Unit = {
    var ss = Array("a","b")
    val s2 =ss.map{x=>new UuidStat(x,1)}

    val sc = new SparkContext(sparkConf)
    val sqlC= new SQLContext(sc)
    var s3 = sqlC.createDataFrame(s2)

    var s4 =sqlC.createDataFrame(s2.toList, classOf[UuidStat])

    LOG.warn("aaaaaaaaaaaaaaaa ",s3.take(2) )
    LOG.warn("aaaaaaaaaaaaaaaa ",s4.take(2) )

    s3.show()
    s4.show()

    //    val res = sqlC.createDataFrame(
//      ss.map{x=> new UuidStat(x, 1)}.toList,
//      classOf[UuidStat]
//    )
//    res

//    import sqlC.implicits._
//    var s =  sc.parallelize(List( Seq("AS","QWE") ,  Seq("AS","QWE") )).toDF()
//    s.show(1)
//    println(DataFrameUtil.showString(s))



    //    testSch
//    hiveContext.read.table("ssp_report_campaign_dm").where(""" b_date = "2017-10-17" """).repartition(1).write.format("csv").save("/pluggable/test/csv2")

    //.insertInto("ssp_report_campaign_dm_spark_csv")
//    testJoin
//    testPublisherThirdIncome

//    mysql2hive

//      println("-------------------" +samCount)
//    hiveContext.sql("show tables").show();
//
//    hiveContext.sql("insert into table dual select * from dual")
//    hiveContext.sql("insert overwrite table dual select count(1)  from dual")



  }

  def testSch(){
//    hiveContext.sql("set spark.sql.caseSensitive=true")
//    hiveContext .read.table("ssp_report_publisher_dm").printSchema()
    hiveContext .read.table("ssp_report_publisher_third_income_dm").printSchema()
  }

  def testJoin ={

    val sc = new SparkContext(sparkConf)
    val sqlC= new SQLContext(sc)

    import collection.JavaConverters._

    val rows= (0 until 5  ).map{x=>
        Row("12")
    }.asJava

    val df1=sqlC.createDataFrame(rows, StructType(StructField("imei",        StringType) :: Nil)).alias("df1")

    val df2=sqlC.createDataFrame(rows, StructType(StructField("imei",        StringType) :: Nil)).as("df2")


    println("ppppppppppppppppppp "+ df1.join(df2,expr("df1.imei = df2.imei"), "inner").count())
    df1.join(df2,expr("df1.imei = df2.imei"), "inner").show()

    println("ppppppppppppppppppp2 "+ df1.join(df2,expr("df1.imei = df2.imei"), "cross").count())
    println("ppppppppppppppppppp3 "+ df1.join(df2,expr("df1.imei = df2.imei"), "outer").count())
    println("ppppppppppppppppppp4 "+ df1.join(df2,expr("df1.imei = df2.imei"), "full").count())
    println("ppppppppppppppppppp5 "+ df1.join(df2,expr("df1.imei = df2.imei"), "full_outer").count())
    println("pppppppppppppppppp6 "+ df1.join(df2,expr("df1.imei = df2.imei"), "left").count())
    println("pppppppppppppppppp7 "+ df1.join(df2,expr("df1.imei = concat(df2.imei,'ww')"), "left_outer").count())
    df1.join(df2,expr("df1.imei = df2.imei"), "left_outer").show
    println("pppppppppppppppppp8 "+ df1.join(df2,expr("df1.imei = df2.imei"), "right").count())
    println("pppppppppppppppppp9 "+ df1.join(df2,expr("df1.imei = df2.imei"), "right_outer").count())
    println("pppppppppppppppppp10 "+ df1.join(df2,expr("df1.imei = df2.imei"), "left_semi").count())
    println("pppppppppppppppppp11 "+ df1.join(df2,expr("df1.imei = df2.imei"), "left_anti").count())
  }

  def samCount:Boolean ={
    val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
    val rdbUser = "root"
    val rdbPassword= "@dfei$@DCcsYG"

    val rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

    var t = "OFFER"
    var mysqlTable = "OFFER"
    var hiveCount = hiveContext
      .sql(s"select count(1) from $t")
      .first()
      .getAs[Long](0)

    var mysqlCount = hiveContext
      .read
      .jdbc(rdbUrl, mysqlTable, rdbProp)
      .selectExpr("count(1)")
      .first()
      .getAs[Long](0)

    println("mysqlCount is %l,hiveCount is %l",mysqlCount, hiveCount)
    (hiveCount == mysqlCount)
  }

  def mysql2hive = {


    val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
    val rdbUser = "root"
    val rdbPassword= "@dfei$@DCcsYG"

    val rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

    val vv= hiveContext
      .read
      .jdbc(rdbUrl, "OFFER", rdbProp)
      .coalesce(1)

    println("countt"+vv.count)

      vv.write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .insertInto("offer")

    hiveContext.sql("show tables").show()
  }

  def testPublisherThirdIncome = {


    val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp_stat?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
    val rdbUser = "root"
    val rdbPassword= "@dfei$@DCcsYG"

    val rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

//    val vv= hiveContext
//      .read
//      .jdbc(rdbUrl, "THIRD_INCOME_201709", rdbProp)
//      .coalesce(1)
//      .where(s"statDate = '2017-09-02'")
//      .coalesce(1)
//      .selectExpr("JarCustomerId", "AppId", "CountryId", "Pv", "ThirdFee", "ThirdSendFee", "StatDate as statDate")
//
//      vv.printSchema()

    val ht=hiveContext.read
     .table("ssp_report_app_third_income")

    import collection.JavaConverters._
    val a = hiveContext.createDataFrame(List(Row (1,1, 2, 2L, new java.math.BigDecimal(2.2), new java.math.BigDecimal(2.1),"2012")).asJava , ht.schema)

    a.write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .insertInto("ssp_report_app_third_income")

    hiveContext.sql("show tables").show()
  }


}
