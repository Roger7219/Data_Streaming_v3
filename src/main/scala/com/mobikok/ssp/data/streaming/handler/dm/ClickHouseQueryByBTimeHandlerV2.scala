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
class ClickHouseQueryByBTimeHandlerV2 extends Handler {

  //view, consumer, topics
  private var viewConsumerTopics = null.asInstanceOf[Array[(String, String, String, String, Array[String],/*Boolean,*/ String, String)]]

//  val SYNC_HANDLER_B_TIME_COUNT = 2
  @volatile var LOCK = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { item =>
      val config = item.toConfig
      val hiveView = config.getString("view")
      val ckTable = if(config.hasPath("ck")) config.getString("ck") else hiveView.replace("_v2", "")
      val minBtExpr = if(config.hasPath("b_time.min")) config.getString("b_time.min") else "'0000-00-01 00:00:00'"
      val consumer = config.getString("message.consumer")
//      val isDay = if(config.hasPath("day")) config.getBoolean("day") else false
      val topics = config.getStringList("message.topics").toArray(new Array[String](0))
      val partialCommitTopicForUpdateCK = s"ck_partial_commit_topic_for_update_$ckTable"
      val partialCommitConsumerForUpdateCK = s"ck_partial_commit_cer_for_update_$ckTable"
      //      LOG.warn("consumer topic", s"($view, $consumer, $topic)")
      (hiveView, ckTable, minBtExpr, consumer, topics, /*isDay,*/ partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK)
    }.toArray

    viewConsumerTopics.foreach{case(_, _, _, consumer, topics, _, _)=>
      if(ArgsConfig.Value.OFFSET_LATEST.equals(argsConfig.get(ArgsConfig.OFFSET))){
        messageClient.setLastestOffset(consumer, topics)
      }
    }
  }

  override def doHandle(): Unit = {

    // 上个批次还在处理时，等待上一批次结束
    LOCK.synchronized{

      LOG.warn("ClickHouseBTimeHandler handler starting")

      RunAgainIfError.run {
        viewConsumerTopics.par.foreach { case(hiveView, ckTable, minBtExpr, consumer, topics, /*isDay, */partialCommitTopicForUpdateCK, partialCommitConsumerForUpdateCK) =>

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

            val minBt = sql(s"select $minBtExpr as min_b_time").first().getAs[String]("min_b_time")
            val filterMS = bs.filter{x=>x.value >= minBt}
            LOG.warn(s"ClickHouseBTimeHandler update b_time(s), count: ${bs.length}", "ckTable", ckTable, "hiveView", hiveView, "all_b_time(s)", bs, "filtered_b_time(s)", filterMS)

            clickHouseClient.overwriteByBTime(ckTable, hiveView, filterMS.map{_.value})

            // 局部提交，只提交这部分b_time对应的偏移
            bs
          })
        }
        LOG.warn("ClickHouseQueryBTimeHandler handler done")
      }
    }

  }
}
