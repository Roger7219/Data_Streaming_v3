package com.mobikok.ssp.data.streaming.handler.dm

import java.util.Date


/**
  * Created by admin on 2017/9/4.
  */
class Test extends Handler{
  class ThreadDemo(threadName:String) extends Runnable{
     def run(){
       LOG.warn("currentThread Name ------- : " + Thread.currentThread.getName())
       val ms=(1 to 10000000)
        .map {x=>
          """{"appId":1165,"jarId":100000,"jarIds":[100000,100003,100004],"publisherId":1021,"imei":"355228081678433","imsi":"414051025289818","installType":0,"sv":"v1.0.0","androidId":"b7e90ef454fa4c52","userAgent":"Mozilla/5.0 (Linux; Android 6.0.1; SM-G610F Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36","connectType":2,"createTime":"2017-09-13 14:40:20","countryId":136,"carrierId":410,"ipAddr":"74.50.214.28","deviceType":1,"pkgName":"com.game.HungryFish.KY","clickId":"b736da0d-d6be-48f2-8850-8dfe478a8468","reportPrice":0.0,"pos":1,"affSub":"000MTest"}"""
        }.toArray
       LOG.warn("kafka send starting", new Date().toLocaleString)
       val st = new Date().getTime

//       ms.foreach{x=>
//         kafkaClient.send("kafkatest", x)
//       }
       kafkaClient.sendToKafka("kafkatest", ms:_*)

       LOG.warn("kafka send using time:",  (new Date().getTime - st) / 1000 + "s")

        println(threadName)
        Thread.sleep(100)
    }
  }

  override def handle(): Unit = {

//    val ms=(1 to 10000000)
//      .map {x=>
//        """{"appId":1165,"jarId":100000,"jarIds":[100000,100003,100004],"publisherId":1021,"imei":"355228081678433","imsi":"414051025289818","installType":0,"sv":"v1.0.0","androidId":"b7e90ef454fa4c52","userAgent":"Mozilla/5.0 (Linux; Android 6.0.1; SM-G610F Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36","connectType":2,"createTime":"2017-09-13 14:40:20","countryId":136,"carrierId":410,"ipAddr":"74.50.214.28","deviceType":1,"pkgName":"com.game.HungryFish.KY","clickId":"b736da0d-d6be-48f2-8850-8dfe478a8468","reportPrice":0.0,"pos":1,"affSub":"000MTest"}"""
//      }.toArray
//    LOG.warn("kafka send starting", new Date().toLocaleString)
//    val st = new Date().getTime
//
//
//    kafkaClient.send("kafkatest_topic", ms:_*)
//
//    LOG.warn("kafka send using time:",  (new Date().getTime - st) / 1000 + "s")

    //com.mobikok.ssp.data.streaming.client.Kalfka_Test.run(kafkaClient)


      new Thread(new ThreadDemo("TestKalfka")).start()


      Thread.sleep(1000000000000L)
  }
}
