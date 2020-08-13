package com.mobikok.ssp.data.streaming.client.cookie

import java.util

import com.mobikok.ssp.data.streaming.entity.{OffsetRange, TopicPartition}
import com.mobikok.ssp.data.streaming.transaction.TransactionCookie

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class KafkaTransactionCookie (_parentId: String,
                              _id: String
                             ) extends TransactionCookie(_parentId, _id) {

}
