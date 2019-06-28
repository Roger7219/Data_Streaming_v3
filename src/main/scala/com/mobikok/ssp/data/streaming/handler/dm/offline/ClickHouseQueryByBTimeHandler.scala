package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{CSTTime, OM, RunAgainIfError, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

class ClickHouseQueryByBTimeHandler extends Handler {

  //view, consumer, topics
  private var viewConsumerTopics = null.asInstanceOf[Array[(String, String, String, String, Array[String])]]

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { item =>
      val config = item.toConfig
      val hiveView = config.getString("view")
      val ckTable = if(config.hasPath("ck")) config.getString("ck") else hiveView
      val minBtExpr = if(config.hasPath("b_time.min")) config.getString("b_time.min") else "'0000-00-01 00:00:00'"
      val consumer = config.getString("message.consumer")
      val topics = config.getStringList("message.topics").toArray(new Array[String](0))
//      LOG.warn("consumer topic", s"($view, $consumer, $topic)")
      (hiveView, ckTable, minBtExpr, consumer, topics)
    }.toArray
  }

  override def handle(): Unit = {

    LOG.warn("ClickHouseBTimeHandler handler starting")
    RunAgainIfError.run {
      viewConsumerTopics.foreach { case(hiveView, ckTable, minBtExpr, consumer, topics) =>
        val pageData = messageClient
          .pullMessage(new MessagePullReq(consumer, topics))
          .getPageData

        var ms = pageData.map { data =>
          OM.toBean(data.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]] {})
        } .flatMap { data => data }
          .flatMap { data => data }
          .filter { partition => "b_time".equals(partition.name) && !"__HIVE_DEFAULT_PARTITION__".equals(partition.value) && StringUtil.notEmpty(partition.value) }
          .distinct
          .sortBy(_.value)(Ordering.String.reverse)
          .toArray

        // 目前只支持小时过滤
        //ms = filterHistoricalBTime(ms)

        var minBt = sql(s"select $minBtExpr as min_b_time").first().getAs[String]("min_b_time")
        var filtereMS = ms.filter{x=>x.value >= minBt}

        LOG.warn(s"ClickHouseBTimeHandler update b_time(s), count: ${ms.length}", "all_b_time(s)", ms, "filtered_b_time(s)", filtereMS)

        clickHouseClient.overwriteByBTime(ckTable.replace("_v2", ""), hiveView, filtereMS.map{_.value})
        messageClient.commitMessageConsumer(
          pageData.map{data =>
            new MessageConsumerCommitReq(consumer, data.getTopic, data.getOffset)
          }:_*
        )

        //通知天粒度modole 聚合天粒度数据
        var ms = pageData.map { data =>
          messageClient.pushMessage(new MessagePushReq(hiveView, data.getKeyBody))
        }


      }

      LOG.warn("ClickHouseQueryBTimeHandler handler done")
    }

  }

  def filterHistoricalBTime(partitions: Array[HivePartitionPart]): Array[HivePartitionPart] = {
    val consumerAndTopic = viewConsumerTopics.map{ each =>
      val topic = each._5.filter{ topic => topic.contains("ck_report_overall")}.head
      (each._2, Array(s"${topic}_finished"))
    }.head

    // 距离当前一个小时的任务不会被过滤
    val neighborBTime = CSTTime.neighborTimes(CSTTime.now.time(), 1.0, 1)

    val finishedBTime = messageClient
      .pullMessage(new MessagePullReq(consumerAndTopic._1, consumerAndTopic._2))
      .getPageData
      .map{ data => OM.toBean(data.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]] {})}
      .flatMap{ data => data }
      .flatMap{ data => data }
      .filter{ partition => "b_time".equals(partition.name) && !neighborBTime.contains(partition.value) }
      .map{ partition => partition.value }
      .distinct

    partitions.filter{ partition => !finishedBTime.contains(partition.value)}
  }
}
