package com.mobikok.ssp.data.streaming.handler.dm.offline

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{OM, RunAgainIfError, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/9/4.
  */
class GoogleBigQueryByBDateHandler extends Handler {

  //view, consumer, topics
  var viewConsumerTopics = null.asInstanceOf[Array[(String, String, Array[String])]]

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

  override def handle (): Unit = {
    LOG.warn("GoogleBigQueryHandler handler starting")

    viewConsumerTopics.foreach{ x=>

      val pd = messageClient
        .pullMessage(new MessagePullReq(x._2, x._3))
        .getPageData

      val ms = pd.map{x=>
          OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
        }
        .flatMap{x=>x}
        .flatMap{x=>x}
//        .filter{x=>"b_time".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  }
        .filter{x=>"b_date".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  } //xxx
        .distinct
        .toArray

      ms.foreach{y=>
        RunAgainIfError.run{
          bigQueryClient.overwriteByBDate(x._1, x._1, y.getValue)
        }

//        var b = true
//        while(b) {
//          try {
//            bigQueryClient.overwriteByBDate(x._1, x._1, y.getValue)
//            b = false
//          }catch {case e:Exception=>
//            LOG.warn("GoogleBigQueryHandler handler fail, Will retry !!", e)
//          }
//        }
      }

      messageClient.commitMessageConsumer(
        pd.map {d=>
          new MessageConsumerCommitReq(x._2, d.getTopic, d.getOffset)
        }:_*
      )

    }

    LOG.warn("GoogleBigQueryHandler handler done")
  }


}


