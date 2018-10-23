package com.mobikok.ssp.data.streaming.client

import java.io.File
import java.util.{Date, Properties}

import com.mobikok.ssp.data.streaming.App.config
import com.mobikok.ssp.data.streaming.module.support.AlwaysTransactionalStrategy
import com.mobikok.ssp.data.streaming.util.CSTTime
import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.htrace.impl.AlwaysSampler
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.reflect.io.Path

/**
  * Created by admin on 2017/9/13.
  */
object Kalfka_Test {



  private  val BROKERLIST =  "node30:6667,node31:6667,node32:6667"
  private  val TEST_TOPIC = "kafkatest_topic"

  //set  kalfka prop
  private val props = new Properties()
//  props.put("metadata.broker.list",this.BROKERLIST)
  props.put("bootstrap.servers",this.BROKERLIST)
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")

  //create Producer
  //private val config = new ProducerConfig(this.props)
  private val producer = new KafkaProducer[String, String](props)

  val config = ConfigFactory.parseFile(new File("E:\\yuanma\\datastreaming\\src\\main\\resources\\agg\\test2\\dw.test.conf"))
  val kafkaClient = new KafkaClient("", config, new MixTransactionManager(
    config,
    new AlwaysTransactionalStrategy(CSTTime.formatter("yyyy-MM-dd HH:00:00"), CSTTime.formatter("yyyy-MM-dd 00:00:00"))
  ))

  //produce mesg and send mesg

  def run(kafkaClient:KafkaClient): Unit = {

      val st = new Date().getTime
//      producer.send((1 to 10000000).map { x =>
//        new ProducerRecord[String, String](TEST_TOPIC, """{"appId":1165,"jarId":100000,"jarIds":[100000,100003,100004],"publisherId":1021,"imei":"355228081678433","imsi":"414051025289818","installType":0,"sv":"v1.0.0","androidId":"b7e90ef454fa4c52","userAgent":"Mozilla/5.0 (Linux; Android 6.0.1; SM-G610F Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36","connectType":2,"createTime":"2017-09-13 14:40:20","countryId":136,"carrierId":410,"ipAddr":"74.50.214.28","deviceType":1,"pkgName":"com.game.HungryFish.KY","clickId":"b736da0d-d6be-48f2-8850-8dfe478a8468","reportPrice":0.0,"pos":1,"affSub":"000MTest"}""")
//      }.toArray: _*)

      kafkaClient.sendToKafka("test_932_topic",
        (1 to 20000).map{ x => """{"appId":1165}"""}.toArray: _*)

//    """{"appId":1165,"b_data":"2017_09_30","l_time":"2017_09_30"}""")

//      ( 1 to 10000).map { x =>
//        producer.send(new ProducerRecord[String, String]("test_931_topic", """{"appId":1165,"b_data":"2017_09_30","l_time":"2017_09_30"}"""))
//      }

      println("using time:" + (new Date().getTime - st) / 1000 + "s")


//    }catch {
//        case e : Exception => println(e)
//      }

//      try{
//        Thread.sleep(3000)
//      }catch {
//        case e : Exception => println(e)
//      }


  }

  def main(args: Array[String]): Unit = {

    //run command --
    // java -jar data-streaming.jar  com.mobikok.ssp.data.streaming.client.Kalfka_Test
//    if(args.length < 2)
//      println("Usage : Kalfka_Test master:9092,worker1:9092 test_topic")

//    run(kafkaClient)
   // Thread.sleep(Long.MaxValue)
  }
}
//(java -jar  /root/samuelson/kafkatest/data-streaming.jar &)  >> log ; tail -f log