package com.mobikok.ssp.data.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.Minutes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object spark_streaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node30:6667,node31:6667,node32:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("topic_ad_click", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.map(record =>record.value)
          .flatMap { _.split(" ") }
          .map { (_, 1) }
          .reduceByKeyAndWindow(_ + _, _ - _, Minutes(2), Seconds(2), 2)
          .print()
    //    val lines = stream.map(_._2)
    //    val words = lines.flatMap(_.split(" "))
    //    val pair = words.map(x => (x, 1))
    //    val wordCounts = pair.reduceByKeyAndWindow(_ + _, _ - _, Minutes(2), Seconds(2), 2)
//    stream.print
    ssc.checkpoint("/words/checkpoint2")
    ssc.start
    ssc.awaitTermination
  }
}