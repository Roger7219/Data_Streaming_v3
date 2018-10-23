package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2018/8/28.
  */
case class LatestOffsetRecord(@BeanProperty var module: scala.Predef.String,
                              @BeanProperty var topic: scala.Predef.String,
                              @BeanProperty var partition: scala.Int,
                              @BeanProperty var untilOffset: scala.Long) extends JSONSerializable{

  def this(){
    this(null, null, 0, 0L)
  }

}
