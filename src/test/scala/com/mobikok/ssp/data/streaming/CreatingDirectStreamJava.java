package com.mobikok.ssp.data.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CreatingDirectStreamJava {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws InterruptedException {
		
		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
		JavaStreamingContext  streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));
		SQLContext sqlContext = new SQLContext(streamingContext.sparkContext());

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "node30:6667,node31:6667,node32:6667");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream3");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("topic_ad_click"/*, "topicB"*/);

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
		    streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

		stream.mapToPair(
		  new PairFunction<ConsumerRecord<String, String>, String, String>() {
		    @Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
		    	System.out.println("ffffffffffffffffffffffffffffffffffffffffffffed");
		      return new Tuple2<String, String>(record.key(), record.value());
		    }
		  }).map(new Function<Tuple2<String,String>, String>() {

			public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
				System.out.println("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeed");
				return stringStringTuple2._2();
			}
		}).print();
		
//		stream.print();
		streamingContext.checkpoint("/words/checkpoint");
		streamingContext.start();  
		streamingContext.awaitTermination(); 
	}
}
