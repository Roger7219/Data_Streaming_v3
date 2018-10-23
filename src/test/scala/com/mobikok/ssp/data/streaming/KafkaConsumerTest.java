package com.mobikok.ssp.data.streaming;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaConsumerTest {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
//      .setMaster("spark://localhost:7077")
       .setMaster("local[1]")
      .setAppName("KafkaConsumerTest")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "100");
    final String topic = "Test1";
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(5) );
    
    Map<String, Object> kafkaParams = new HashMap<String, Object>();
//    kafkaParams.put("bootstrap.servers", "104.250.136.138:6666,104.250.133.18:6666,104.250.131.162:6666");
    kafkaParams.put("bootstrap.servers", "104.250.136.138:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", false);

//    Collection<String> topics = Arrays.asList(topic);
    
    Collection<TopicPartition> topics = Arrays.asList(
    		new TopicPartition(topic,0)/*,new TopicPartition(topic,1),new TopicPartition(topic,2)
    		*/);
    Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>() {{
    	put(new TopicPartition(topic,0), 0L);
    }};



    JavaInputDStream<ConsumerRecord<String, String>> stream =
      KafkaUtils.createDirectStream(
    		  jsc,
    		  LocationStrategies.PreferConsistent(),
    		  ConsumerStrategies.<String, String>Assign(topics,kafkaParams, offsets )
//    		  ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
      );

    stream.map(new Function<ConsumerRecord<String,String>, Tuple2<String, String>>() {
        public Tuple2<String, String> call(ConsumerRecord<String, String> v1) throws Exception {
            return new Tuple2<String, String>(v1.key(), v1.value());
        }
    }).print();
    //.//mapToPair(record -> new Tuple2<Object, Object>(record.key(), record.value()))

    jsc.start();
    jsc.awaitTermination();
//    .foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
//		
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void call(JavaPairRDD<String, String> arg0) throws Exception {
//			System.out.println(arg0);
//			
//		}
//	});;
	}
}
