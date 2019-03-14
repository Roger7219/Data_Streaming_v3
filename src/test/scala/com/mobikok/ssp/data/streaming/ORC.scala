package com.mobikok.ssp.data.streaming

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneOffset}
import java.util.Date

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object ORC {

  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", """D:\lxl\hadoop-2.6.0""")
//    System.setProperty("hive.exec.scratchdir", """D:\lxl\hadoop-2.6.0\hive\pluggable""")
//    System.setProperty("spark.local.dir", """D:\lxl\hadoop-2.6.0\spark\pluggable""")


    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    val conf = new SparkConf().set("hive.exec.dynamic.partition.mode","nonstrict").setAppName("wordcount")//.setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(3*60))
    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(2))
    val hiveContext = new HiveContext(ssc.sparkContext)

    val sqlContext = new SQLContext(ssc.sparkContext)


    val kafkaParams = scala.Predef.Map[String, Object](
      "bootstrap.servers" -> "node30:6667,node31:6667,node32:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream2",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("topic_ad_click", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
      }

    val dataFormat : SimpleDateFormat = new SimpleDateFormat("yyyyMMdd-000000")

    stream.foreachRDD{ rdd =>
      try {
        val l_date = dataFormat.format(new Date())

        val nr = rdd.map{ x=>
            // println(s"1>>> $c")
            x.value();
          }.filter{ x=>
            x.startsWith("""{"""")
          }.map{ x=>
              val r = x.substring("{".length)
              s"""{"l_date": "$l_date", $r"""
          }
        // println("===")
        hiveContext.sql("show tables").show();
        println("ffffffffffffffffffffffffffffnd")
        nr.take(1).foreach { x =>
            println(s"2>>> $x")
        }
        //x.collect()
        var data = sqlContext.read.json(nr)

        data.printSchema()

        data.createOrReplaceTempView("mytable")
        val myres=sqlContext.sql("SELECT * FROM mytable limit 10")
        myres.show()

//        val schema = StructType(StructField("id", IntType) :: StructField("publisherId", IntType) :: Nil)

        val schema: StructType = StructType(
            StructField("id",          IntegerType) ::
            StructField("publisherId", IntegerType) ::
            StructField("subId",       IntegerType) ::
            StructField("offerId",     IntegerType) ::
            StructField("campaignId ", IntegerType) ::
            StructField("countryId ",  IntegerType) ::
            StructField("carrierId  ", IntegerType) ::
            StructField("deviceType ", IntegerType) ::
            StructField("userAgent",   StringType)  ::
            StructField("ipAddr",      StringType)  ::
            StructField("clickId",     StringType)  ::
            StructField("price",       DoubleType)  ::
            StructField("reportTime",  StringType)  ::
            StructField("createTime",  StringType)  ::
            StructField("clickTime",   StringType)  ::
            StructField("showTime",    StringType)  ::
            StructField("requestType", StringType)  ::
            StructField("priceMethod", IntegerType) ::
            StructField("bidPrice",    DoubleType)  ::
            StructField("adType",      IntegerType) ::
            StructField("isSend",      IntegerType) ::
            StructField("reportPrice", DoubleType)  ::
            StructField("sendPrice",   DoubleType)  ::
            StructField("s1",          StringType)  ::
            StructField("s2 ",         StringType)  ::
            StructField("gaid",        StringType)  ::
            StructField("androidId",   StringType)  ::
            StructField("idfa",        StringType)  ::
            StructField("postBack",    StringType)  ::
            StructField("sendStatus",  StringType)  ::
            StructField("sendTime",    StringType)  ::
            StructField("sv",          StringType)  ::
            StructField("imei",        StringType)  ::
            StructField("imsi",        StringType)  ::
            StructField("l_date",      StringType)  :: Nil)

        hiveContext.read.schema(schema).json(nr).write.mode(SaveMode.Append).format("orc").insertInto("ad_click_dwi_spark2")//.save("/pluggable/ad_click_dwi_spark")//.insertInto("ad_click_dwi_spark")

        //sqlContext.read.json("/pluggable/json").printSchema()
        //          sqlContext.read.json("/pluggable/json").foreach{ x=>
        //            print(x)
        //          }
        //          sqlContext.read.json()
      }
      catch {
        case t: Throwable => t.printStackTrace();println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&") // TODO: handle error
      }
      //hiveContext.read.json(x)//.write.mode(SaveMode.Overwrite).format("orc").save("/pluggable/ad_click_dwi_spark")//.insertInto("ad_click_dwi_spark")
    }


    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeend")
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    // ssc.checkpoint("/words/checkpoint2")
    ssc.start
    ssc.awaitTermination
  }
}