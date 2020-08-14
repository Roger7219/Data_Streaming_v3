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
  * Created by admin on 2017/9/4.
  */
class GoogleBigQueryBTimeHandlerV2 extends Handler {

  //view, consumer, topics
  var viewConsumerTopics = null.asInstanceOf[Array[(String, String, String, Array[String], String, String)]]

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val hiveView = c.getString("view")
      val bigQueryTable = hiveView // BiQuery表和hive视图同名
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      val partialCommitTopicForUpdateCK = s"bq_partial_commit_topic_for_update_$bigQueryTable"
      val partialCommitConsumerForUpdateCK = s"bq_partial_commit_cer_for_update_$bigQueryTable"
      (hiveView, bigQueryTable, mc, mt, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK)
    }.toArray
  }

  override def doHandle (): Unit = {
    LOG.warn("GoogleBigQueryBTimeHandler handler starting")

    RunAgainIfError.run({
      viewConsumerTopics.foreach{ case(hiveView, bigQueryTable, consumer, topics, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK)=>

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

          LOG.warn(s"GoogleBigQueryBTimeHandlerV2 update b_time(s), count: ${bs.length}", "bigQueryTable", bigQueryTable, "hiveView", hiveView, "b_time(s)", bs)


          bigQueryClient.overwriteByBTime(bigQueryTable, hiveView, bs.map{_.value})

          // 局部提交，只提交这部分b_time对应的偏移
          bs
        })

      }
    })

    LOG.warn("GoogleBigQueryBTimeHandler handler done")
  }


}


