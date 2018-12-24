package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util.HashMap
import java.util.regex.Pattern

import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.{CSTTime, HttpUtils}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2018/8/06 0036.
  */
class KafkaBrokerCheckHandler extends Handler{

  val topic = "kafka_broker_check_topic"

  var monitorClient: MonitorClient = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient ,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName,bigQueryClient, greenplumClient, rDBConfig,kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    monitorClient = new MonitorClient(messageClient)

  }


  override def handle(): Unit = {

    LOG.warn(s"KafkaBrokerCheckHandler handle start")

    val url: String = "http://node15:9000/clusters/AloneKafkaCluster"

    val responseBody = HttpUtils.sendGet(url, new HashMap[String, String])

//    LOG.warn(s"KafkaBrokerCheckHandler ", "responseBody: ", responseBody)

    var brokerNum: Int = 0

    val p = Pattern.compile("<a href=\"/clusters/AloneKafkaCluster/brokers\">(\\d+)</a>")

    val m = p.matcher(responseBody)

    while ( m.find) {

      brokerNum = m.group(1).toInt

    }

    LOG.warn(s"KafkaBrokerCheckHandler ", "topic: ", topic, "brokerNum: ", brokerNum)
    monitorClient.push(new MonitorMessage(topic, CSTTime.now.time(), brokerNum))

    LOG.warn(s"KafkaBrokerCheckHandler handle end")
  }

}

object dx{
  def main(args: Array[String]): Unit = {


    var brokerNum: Int = 0

    val p = Pattern.compile("<a href=\"/clusters/AloneKafkaCluster/brokers\">(\\d+)</a>")

    val m = p.matcher("<a href=\"/clusters/AloneKafkaCluster/brokers\">113</a>")

    while ( m.find) {

      brokerNum = m.group(1).toInt

    }

    println(brokerNum)

  }
}
