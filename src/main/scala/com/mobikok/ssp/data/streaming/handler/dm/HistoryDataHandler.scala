package com.mobikok.ssp.data.streaming.handler.dm

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{OM, RunAgainIfError, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

class HistoryDataHandler extends Handler {

  //view, consumer, topics
  var viewConsumerTopics: Array[(String, String, Array[String])] = null.asInstanceOf[Array[(String, String, Array[String])]]

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val v = c.getString("view")
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      (v, mc, mt)
    }.toArray
  }

  override def handle(): Unit = {
    LOG.warn("HistoryDataHandler handler starting")
    RunAgainIfError.run{
      viewConsumerTopics.foreach{ topics =>
        val pd = messageClient
          .pullMessage(new MessagePullReq(topics._2, topics._3))
          .getPageData

        val ms = pd.map{x=>
          OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
        }.flatMap{x=>x}
          .flatMap{x=>x}
          //        .filter{x=>"b_time".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  }
          .filter{x=>"b_time".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  } //xxx
          .distinct
          .sortBy(_.value)(Ordering.String.reverse)
          .toArray

        LOG.warn(s"HistoryDataHandler update b_time(s), count: ${ms.length}", ms)
        // TODO import here
        hiveContext.read
            .table(topics._1)
            .selectExpr()
            .groupBy()
            .agg(expr(""))
            .write
            .format("ORC")
            .insertInto(s"${topics._1}_accday")

        messageClient.commitMessageConsumer(
          pd.map {d=>
            new MessageConsumerCommitReq(topics._2, d.getTopic, d.getOffset)
          }:_*
        )
      }
    }
  }
}
