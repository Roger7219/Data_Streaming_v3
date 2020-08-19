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
class GoogleBigQueryUploadBTimeHandler extends Handler {

  //view, bigQueryTable, consumer, topics, partialCommitTopicForUpdateBQ, partialCommitConsumerForUpdateBQ
  var dmViews: Array[(String, String, String, Array[String], String, String)] = null

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    dmViews = handlerConfig.getObjectList("items").map { x =>
      val config = x.toConfig
      val hiveView = config.getString("view")
      val bigQueryTable = if(config.hasPath("bq")) config.getString("bq") else hiveView //如果没有配置BigQuery表，那hive视图名就是BigQuery表名
      val mc = config.getString("message.consumer")
      val mt = config.getStringList("message.topics").toArray(new Array[String](0))
      val partialCommitTopicForUpdateBQ = s"bq_partial_commit_topic_for_update_$bigQueryTable"
      val partialCommitConsumerForUpdateBQ = s"bq_partial_commit_cer_for_update_$bigQueryTable"
      (hiveView, bigQueryTable, mc, mt, partialCommitTopicForUpdateBQ, partialCommitConsumerForUpdateBQ)
    }.toArray

    dmViews.foreach{case(_, _, consumer, topics, _, _)=>
      if(ArgsConfig.Value.OFFSET_LATEST.equals(argsConfig.get(ArgsConfig.OFFSET))){
        messageClient.setLastestOffset(consumer, topics)
      }
    }
  }

  override def doHandle (): Unit = {
    LOG.warn("GoogleBigUploadBTimeHandler handler start")

    RunAgainIfError.run({
      dmViews.foreach{ case(hiveView, bigQueryTable, consumer, topics, partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK)=>

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

          LOG.warn(s"GoogleBigUploadBTimeHandler update b_time(s), count: ${bs.length}", "bigQueryTable", bigQueryTable, "hiveView", hiveView, "b_time(s)", bs)


          bigQueryClient.overwriteByBTime(bigQueryTable, hiveView, bs.map{_.value})

          // 局部提交，只提交这部分b_time对应的偏移
          bs
        })
      }
    })
    LOG.warn("GoogleBigUploadBTimeHandler handler done")
  }
}