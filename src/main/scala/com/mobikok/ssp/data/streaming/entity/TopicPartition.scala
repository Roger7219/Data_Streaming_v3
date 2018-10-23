package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/20.
  */
case class TopicPartition (@BeanProperty topic: String,
                           @BeanProperty partition: Int) extends JSONSerializable{

}