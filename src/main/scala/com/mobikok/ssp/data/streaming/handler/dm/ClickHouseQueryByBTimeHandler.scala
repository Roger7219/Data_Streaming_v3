package com.mobikok.ssp.data.streaming.handler.dm

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{OM, RunAgainIfError, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

class ClickHouseQueryByBTimeHandler extends Handler {

  val ckHosts = new util.ArrayList[String]()
  //view, consumer, topics
  private var viewConsumerTopics = null.asInstanceOf[Array[(String, String, Array[String])]]

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { item =>
      val config = item.toConfig
      val view = config.getString("view")
      val consumer = config.getString("message.consumer")
      val topic = config.getStringList("message.topics").toArray(new Array[String](0))
//      LOG.warn("consumer topic", s"($view, $consumer, $topic)")
      (view, consumer, topic)
    }.toArray
  }

  override def handle(): Unit = {

    LOG.warn("ClickHouseBTimeHandler handler starting")
    RunAgainIfError.run {
      viewConsumerTopics.foreach { topic =>
        val pageData = messageClient
          .pullMessage(new MessagePullReq(topic._2, topic._3))
          .getPageData

        val ms = pageData.map { data =>
          OM.toBean(data.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]] {})
        } .flatMap { data => data }
          .flatMap { data => data }
          .filter { partition => "b_time".equals(partition.name) && !"__HIVE_DEFAULT_PARTITION__".equals(partition.value) && StringUtil.notEmpty(partition.value) }
          .distinct
          .sortBy(_.value)(Ordering.String.reverse)
          .toArray

        LOG.warn(s"ClickHouseBTimeHandler update b_time(s), count: ${ms.length}", ms)

//        var ckTable = topic._1
//        if ("ssp_report_overall_dm_day_v2".equals(topic._1)) {
//          ckTable = "ssp_report_overall_dm_day"
//        }
        clickHouseClient.overwriteByBTime(topic._1.replace("_v2", ""), topic._1, ms.map{_.value})
        messageClient.commitMessageConsumer(
          pageData.map{data =>
            new MessageConsumerCommitReq(topic._2, data.getTopic, data.getOffset)
          }:_*
        )

      }

      LOG.warn("ClickHouseQueryBTimeHandler handler done")
    }

  }
}
