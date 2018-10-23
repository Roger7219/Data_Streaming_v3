package com.mobikok.ssp.data.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamingTest {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
		JavaStreamingContext  streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));
		SQLContext sqlContext = new SQLContext(streamingContext.sparkContext());



		streamingContext.checkpoint("/words/checkpoint");
		streamingContext.start();  
		streamingContext.awaitTermination(); 
	}
}
