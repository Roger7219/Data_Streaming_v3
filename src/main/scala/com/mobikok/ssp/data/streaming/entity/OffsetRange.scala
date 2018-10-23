package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/20.
  */
case class OffsetRange (@BeanProperty val topic: scala.Predef.String,
                        @BeanProperty val partition: scala.Int,
                        @BeanProperty val fromOffset: scala.Long,
                        @BeanProperty val untilOffset: scala.Long) extends JSONSerializable{

}
