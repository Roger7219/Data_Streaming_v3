package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by admin on 2017/12/11.
  */
case class ImageInfo(@BeanProperty  id:               Integer,
                     @BeanProperty  todayClick:       Long,
                     @BeanProperty  todayShowCount:   Long,
                     @BeanProperty  ctr:              Double)
 extends JSONSerializable{

}
