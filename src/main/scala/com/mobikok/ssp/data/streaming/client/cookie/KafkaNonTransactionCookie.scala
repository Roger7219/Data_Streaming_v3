package com.mobikok.ssp.data.streaming.client.cookie

import java.util

import com.mobikok.ssp.data.streaming.entity.{OffsetRange, TopicPartition}

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
case class KafkaNonTransactionCookie (_parentId: String,
                                      _id: String
                                     ) extends KafkaTransactionCookie(_parentId, _id) {

}
