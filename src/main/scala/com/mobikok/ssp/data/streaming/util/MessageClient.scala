package com.mobikok.ssp.data.streaming.util

import java.{lang, util}

import com.mobikok.message.{Message, MessagePushReq, Resp}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import JavaMessageClient.{Callback, CallbackRespStrategy, CommitOffsetStrategy, HivePartitionPartsPartialCommitCallback}
import com.mobikok.message.client.MessageClientApi

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object MessageClient{
  private var apiBase: String = _

  def init(apiBase: String): Unit ={
    this.apiBase = apiBase
  }
}
/**
  * 针对MessageClient的封装，
  * 主要是针对流统计相关的分区（b_time,b_date和b_date）消息的封装，便于处理大部分需求。
  */
class MessageClient(loggerName: String) {

  var messageClientApi = new MessageClientApi(loggerName, MessageClient.apiBase)

  def checkInited(): Unit ={
    if(messageClientApi.getApiBase == null) throw new RuntimeException("apiBase cannot be empty! MessageClient.init() needs to be called first to initialize the apiBase")
  }
//
//  def pull(consumer: String, topic: String, callback: List[Message] => Boolean ): Unit ={
//    pulls(consumer, Array(topic), callback)
//  }

  //按offset升序
  def pull (consumer: String, topics: Array[String], callback: List[Message] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndCommit(messageClientApi, consumer, new JavaMessageClient.Callback[Resp[util.List[Message]]]() {
      override def doCallback (resp: Resp[util.List[Message]]): lang.Boolean = {
        callback(resp.getPageData.asScala.toList)
      }
    }, topics:_*)
  }

  def pullUpdatable(consumer: String, topics: Array[String]): String ={
    checkInited
    var result: String = null
    pull(consumer, topics, {ms=>
      result = if(ms.nonEmpty) ms.head.getData else null
      false
    })
    result
  }

  // b_date
  def pullBDateDesc (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByBDateDescHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // b_time & partial commit
  def pullBTimeDescAndPartialCommit(consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Array[HivePartitionPart]): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByBTimeDescHivePartitionParts(messageClientApi, consumer, new HivePartitionPartsPartialCommitCallback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): Array[HivePartitionPart] = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // b_time
  def pullBTimeDesc (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByBTimeDescHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // l_time
  def pullLTimeDesc (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByLTimeDescHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  def pullLTimeDesc (consumer: String,
                     topics:Array[String],
                     callback: List[HivePartitionPart] => Boolean,
                     //提交偏移策略
                     commitOffsetStrategy: (util.Map[HivePartitionPart, util.List[Message]] , util.List[HivePartitionPart] ) => util.Map[HivePartitionPart, util.List[Message]],
                     //传入回调函数值的策略
                     callbackRespStrategy: util.List[HivePartitionPart] => util.List[HivePartitionPart]
                    ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByPartitionFieldDesc(
      "l_time",
      messageClientApi,
      consumer,
      new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
        override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
          callback(resp.asScala.toList)
        }
      }, new CommitOffsetStrategy() {
        override def callback(partitionAndMessageMap: util.Map[HivePartitionPart, util.List[Message]], descHivePartitionParts: util.List[HivePartitionPart]): util.Map[HivePartitionPart, util.List[Message]] = {
          commitOffsetStrategy(partitionAndMessageMap, descHivePartitionParts)
        }
      }, new CallbackRespStrategy(){
        override def callback(callbackResp: util.List[HivePartitionPart]): util.List[HivePartitionPart] = {
          callbackRespStrategy(callbackResp)
        }
      }, topics:_*)
  }

  //获取的是排除第一个的l_time(s)
  def pullLTimeDescTail (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByLTimeDescTailHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }
////  获取的是排除第一个的l_time(s)
//  def pullLTimeDescTailReturnAll (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
//    MessageClientUtil.pullAndSortByLTimeDescTailHivePartitionPartsReturnAll(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
//      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
//        callback(resp.asScala.toList)
//      }
//    }, topics:_*)
//  }

  //获取的是排除第一个的b_time(s)
  def pullBTimeDescTail (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByBTimeDescTailHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  //获取的是排除第一个的b_date(s)
  def pullBDateDescTail (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    JavaMessageClient.pullAndSortByBDateDescTailHivePartitionParts(messageClientApi, consumer, new JavaMessageClient.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  def push(req: PushReq): Unit = {
    checkInited
    messageClientApi.pushMessage(new MessagePushReq(req.topic, req.key))
  }

  def push(reqs: Array[PushReq]): Unit = {
    checkInited
    messageClientApi.pushMessage(reqs.map{ x=> new MessagePushReq(x.topic, x.key)}:_*)
  }

  def push(req: UpdateReq): Unit = {
    checkInited
    messageClientApi.pushMessage(new MessagePushReq(req.topic, "-", true, req.data))
  }

  def push(reqs: Array[UpdateReq]): Unit = {
    checkInited
    messageClientApi.pushMessage(reqs.map{ x=> new MessagePushReq(x.topic, "-", true, x.data)}:_*)
  }

  def setLastestOffset(messageConsumer: String, messageTopics: Array[String]): Unit ={
    checkInited
    pull(messageConsumer,messageTopics, {_=> true})
  }

}

case class UpdateReq(topic: String, data: String)

case class PushReq(topic: String, key: String)