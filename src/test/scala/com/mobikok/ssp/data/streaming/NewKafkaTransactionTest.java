package com.mobikok.ssp.data.streaming;

import java.util.Properties;

import javax.management.RuntimeErrorException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.KafkaException;
//import org.apache.kafka.common.errors.AuthorizationException;
//import org.apache.kafka.common.errors.OutOfOrderSequenceException;
//import org.apache.kafka.common.errors.ProducerFencedException;
//import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by admin on 2017/10/18.
 */
public class NewKafkaTransactionTest {
	private static Logger LOG = LoggerFactory.getLogger(NewKafkaTransactionTest.class);
    public static void main(String[] args) throws InterruptedException {
    	String topic = "Test1";
        Properties props = new Properties();
//        props.put("bootstrap.servers", "node14:6666,node15:6666,node16:6666");
        props.put("bootstrap.servers", "104.250.136.138:9092");
        props.put("acks", "all");
//        props.put("enable.idempotence", "true");
	    props.put("retries", 2);
	    props.put("batch.size", 16384);
	    props.put("linger.ms", 1);
	    props.put("buffer.memory", 33554432);
	    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id", "my-transactional-id2");
        Producer<String, String> producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());
//        producer.initTransactions();
//
//        try {
//            producer.beginTransaction();
//            LOG.warn("kafka test :transaction start!!!!!!!!!");
//            for (int i = 0; i < 20000; i++){
//                producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), "{\"appId\":1165}"));
//                LOG.warn("kafka test :producer.send--" + "key_" + Integer.toString(i));
//            }
//
////            producer.send(new ProducerRecord<>("topic",Integer.toString(1), "value2_" +Integer.toString(1)));
////            producer.send(new ProducerRecord<>("topic", "key2_" + Integer.toString(2), "value2_" +Integer.toString(2)));
//
////            if(true) throw new RuntimeException("asd");
//
//            producer.commitTransaction();
//            LOG.warn("kafka test :producer commitTransaction over!");
//        } catch (ProducerFencedException  e) {
//            // We can't recover from these exceptions, so our only option is to close the producer and exit.
//        	e.printStackTrace();
//            producer.close();
//        }catch (OutOfOrderSequenceException e) {
//            // We can't recover from these exceptions, so our only option is to close the producer and exit.
//            e.printStackTrace();
//            producer.close();
//        }catch (AuthorizationException e) {
//            // We can't recover from these exceptions, so our only option is to close the producer and exit.
//            e.printStackTrace();
//            producer.close();
//        }
//        catch (KafkaException e) {
//            // For all other exceptions, just abort the transaction and try again.
//            producer.abortTransaction();
//            e.printStackTrace();
//        }
//        LOG.warn("kafka test  over!");
//        producer.close();
//        Thread.sleep(5000);
    }
}
