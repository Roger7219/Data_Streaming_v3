package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by admin on 2017/12/11.
  */
case class OfferRoiEcpm(//@BeanProperty  b_date:           String,
                        @BeanProperty  id:               Integer,
                        @BeanProperty  roi:              Double,
                        @BeanProperty  ecpm:             Double)
 extends JSONSerializable{

}
