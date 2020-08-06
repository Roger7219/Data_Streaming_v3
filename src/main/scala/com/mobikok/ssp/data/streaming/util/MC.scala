package com.mobikok.ssp.data.streaming.util

import java.{lang, util}

import com.mobikok.message.{Message, MessagePushReq, Resp}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.MessageClientUtil.{Callback, CallbackRespStrategy, CommitOffsetStrategy, HivePartitionPartsPartialCommitCallback}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * MessageClient / MessageClientUtil for Scala !!<br>
  * Created by Administrator on 2018/1/19.
  */
object MC {

  private var messageClient: MessageClient = null

  def init(messageClient: MessageClient): Unit ={
    this.messageClient = messageClient
  }
  def checkInited(): Unit ={
    if(this.messageClient == null) throw new RuntimeException("MC must be initialized first")
  }
//
//  def pull(consumer: String, topic: String, callback: List[Message] => Boolean ): Unit ={
//    pulls(consumer, Array(topic), callback)
//  }

  //按offset升序
  def pull (consumer: String, topics: Array[String], callback: List[Message] => Boolean ): Unit ={
    checkInited
    MessageClientUtil.pullAndCommit(messageClient, consumer, new MessageClientUtil.Callback[Resp[util.List[Message]]]() {
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
    MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // b_time & partial commit
  def pullBTimeDescAndPartialCommit(consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Array[HivePartitionPart]): Unit ={
    checkInited
    MessageClientUtil.pullAndSortByBTimeDescHivePartitionParts(messageClient, consumer, new HivePartitionPartsPartialCommitCallback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): Array[HivePartitionPart] = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // b_time
  def pullBTimeDesc (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    MessageClientUtil.pullAndSortByBTimeDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  // l_time
  def pullLTimeDesc (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    MessageClientUtil.pullAndSortByLTimeDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
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
    MessageClientUtil.pullAndSortByPartitionFieldDesc(
      "l_time",
      messageClient,
      consumer,
      new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
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
    MessageClientUtil.pullAndSortByLTimeDescTailHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
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
    MessageClientUtil.pullAndSortByBTimeDescTailHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  //获取的是排除第一个的b_date(s)
  def pullBDateDescTail (consumer: String, topics:Array[String], callback: List[HivePartitionPart] => Boolean ): Unit ={
    checkInited
    MessageClientUtil.pullAndSortByBDateDescTailHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
      override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {
        callback(resp.asScala.toList)
      }
    }, topics:_*)
  }

  def push(req: PushReq): Unit = {
    checkInited
    messageClient.pushMessage(new MessagePushReq(req.topic, req.key))
  }

  def push(reqs: Array[PushReq]): Unit = {
    checkInited
    messageClient.pushMessage(reqs.map{ x=> new MessagePushReq(x.topic, x.key)}:_*)
  }

  def push(req: UpdateReq): Unit = {
    checkInited
    messageClient.pushMessage(new MessagePushReq(req.topic, "-", true, req.data))
  }

  def push(reqs: Array[UpdateReq]): Unit = {
    checkInited
    messageClient.pushMessage(reqs.map{ x=> new MessagePushReq(x.topic, "-", true, x.data)}:_*)
  }

  def setLastestOffset(messageConsumer: String, messageTopics: Array[String]): Unit ={
    checkInited
    MC.pull(messageConsumer,messageTopics, {x=> true})
  }

}

case class UpdateReq(topic: String, data: String)

case class PushReq(topic: String, key: String)