package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.net.HttpURLConnection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.OM
import com.mobikok.ssp.data.streaming.util.http.Callback
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created by Administrator on 2017/7/13.
  */
class   KylinHandler extends Handler {

  private val kylinLTimeFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  private val dwLTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val messageClientConsumerPrefix = "KylinHandler"


  var modelOutputDir:String = null
  var messageTopics2kylinCubesGroups: mutable.Buffer[(mutable.Buffer[String], mutable.Buffer[String], Boolean)] = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient, greenplumClient:GreenplumClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient,messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName,bigQueryClient, greenplumClient, rDBConfig, kafkaClient: KafkaClient,messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    messageTopics2kylinCubesGroups = handlerConfig.getConfigList("groups").map{ x=>
      var isFullLTime = false
      try {
        isFullLTime = if ("full".equals(x.getString("kylin.ltime"))) true else false
      }catch {  case e:Exception=>}

      (
        x.getList("message.topics").map{y=>y.unwrapped().toString},
        x.getList("kylin.cubes").map{y=>y.unwrapped().toString},
        isFullLTime
      )
    }
  }

  override def handle (): Unit = {

    if("false".equals(rDBConfig.readRDBConfig(RDBConfig.KYLIN_ENABLE_CREATE_CUBE))) {
      LOG.warn("KylinHandler Skip the handle by rdb config kylin.enable.create.cube = false")
      return
    }

    val h = new Date().getHours
    if(0 <= h && h < 3 ) {
      LOG.warn("KylinHandler Skip the handle from 0 to 3 of time hour")
      return
    }

    messageTopics2kylinCubesGroups.foreach{ x=>

      val messageTopics = x._1
      val kylinCubesGroups = x._2
      val isFull = x._3

      //新建或刷新cube
      kylinCubesGroups.foreach{ cube=>

        val cer = s"${messageClientConsumerPrefix}_$cube"

        //messageClient拉取
        val resp = messageClient.pullMessage(new MessagePullReq(cer, messageTopics.toArray))
        val mps = resp.getPageData.map{x=>
          OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
        }

        val offsets = resp.getPageData.map{x=>
          (cer, x.getTopic) -> x.getOffset
        }.toMap

        val l_times = mps.flatMap{x=>x}.flatMap{x=>x}.filter{x=> "l_time".equals(x.name)}.map{x=>x.value}.distinct
        LOG.warn("KylinHandler messageClient pull message extracted l_times", s"kylinCube: $cube \nmessageTopics: ${util.Arrays.deepToString(messageTopics.toArray)} \nl_times: ${util.Arrays.deepToString(l_times.toArray)}")

        l_times.map {z=>
          var startT, endT:String = null
          //全量刷新
          if(isFull) {
            startT = "2000-01-01 00:00:00"
            endT = "8000-01-01 00:00:00"
          }else {
            startT = kylinLTimeFormat.format(dwLTimeFormat.parse(z))
            //+1天
            endT = kylinLTimeFormat.format(new Date(dwLTimeFormat.parse(z).getTime + 1000*60*60*24))
          }

          LOG.warn("KylinHandler kylinClient refreshOrBuildCube starting", s"cubeName: ${cube}, \nstartTime: ${startT}, \nendTime: ${endT}")
          kylinClientV2.refreshOrBuildCube(cube, startT, endT, new Callback {

            override def prepare (conn: HttpURLConnection): Unit = {}
            override def failed (responseStatus: String, responseError: String, ex: Exception): Unit = {}
            override def completed (responseStatus: String, response: String): Unit = {
              val m = messageTopics.map{topic=>
                new MessageConsumerCommitReq(cer, topic, offsets.get( (cer, topic) ).getOrElse(0L) )
              }
              messageClient.commitMessageConsumer(m:_*)

            }
          }, false)

        }
      }

//      if(!hasKylinErrorResp) {
//        //messageClient提交
//        val m = resp.getPageData.map{x=>
//          new MessageConsumerCommitReq(messageClientConsumer, x.getTopic, x.getOffset)
//        }
//        messageClient.commitMessageConsumer(m:_*)
//        //LOG.warn("KylinHandler messageClient commitMessageConsumer done", m.toArray)
//      }

    }
  }




}

//object X{
//  def main (args: Array[String]): Unit = {
//
//    val bdateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val kylinLTimeFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
//    val dwLTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val endT = kylinLTimeFormat.format(new Date(new Date().getTime + 1000*60*60*24))
//    println(endT)
//
//  }
//}
