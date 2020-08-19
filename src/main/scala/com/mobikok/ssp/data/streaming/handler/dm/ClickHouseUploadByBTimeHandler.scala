package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * 支持局部提交偏移
  */
class ClickHouseUploadByBTimeHandler extends Handler {

  //view, clickhouseTable, consumer, topics, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK
  private var dmViews: Array[(String, String, String, Array[String], String, String)] = null

  @volatile var LOCK = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    dmViews = handlerConfig.getObjectList("items").map { item =>
      val config = item.toConfig
      val hiveView = config.getString("view")
      val ckTable = if(config.hasPath("ck")) config.getString("ck") else hiveView //如果没有配置ClickHouse表，那hive视图名就是ClickHouse表名
      val consumer = config.getString("message.consumer")
      val topics = config.getStringList("message.topics").toArray(new Array[String](0))
      val partialCommitTopicForUpdateCK = s"ck_partial_commit_topic_for_update_$ckTable"
      val partialCommitConsumerForUpdateCK = s"ck_partial_commit_cer_for_update_$ckTable"
      (hiveView, ckTable, consumer, topics, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK)
    }.toArray

    dmViews.foreach{case(_, _, consumer, topics, _, _)=>
      if(ArgsConfig.Value.OFFSET_LATEST.equals(argsConfig.get(ArgsConfig.OFFSET))){
        messageClient.setLastestOffset(consumer, topics)
      }
    }
  }

  override def doHandle(): Unit = {
    LOG.warn("ClickHouseUploadByBTimeHandler handler start")

    RunAgainIfError.run {
      dmViews.foreach { case(hiveView, ckTable, consumer, topics, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK) =>

        // 重新发送消息到新的topic(topicForUpdateCK)，该消息是一个b_time对应一条消息，以便后续只针对某个b_time对应的偏移做commit
        messageClient.pullBTimeDesc(consumer, topics, { x=>
          x.foreach{b_time=>
            //重新发送到用于局部提交的topic
            messageClient.push(new PushReq(partialCommitTopicForUpdateCK, OM.toJOSN(Array(Array(b_time))) ))
          }
          true
        })

        //拉取用于局部提交的topic
        messageClient.pullBTimeDescAndPartialCommit(partialCommitConsumerForUpdateCK, Array(partialCommitTopicForUpdateCK), { bTimes=>

          // 取最前2个+最后1个b_time
          val bs: Array[HivePartitionPart] = if(bTimes.size > 2) bTimes.take(2).toArray :+ bTimes.last else bTimes.take(2).toArray

          LOG.warn(s"ClickHouseUploadByBTimeHandler update b_time(s), count: ${bs.length}", "ckTable", ckTable, "hiveView", hiveView, "b_time(s)", bs)

          clickHouseClient.overwriteByBTime(ckTable, hiveView, bs.map{_.value})

          // 局部提交，只提交这部分b_time对应的偏移
          bs
        })
      }
    }
    LOG.warn("ClickHouseUploadByBTimeHandler handler done")
  }
}