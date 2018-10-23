package com.mobikok.ssp.data.streaming.entity

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable
import scala.beans.BeanProperty

/**
  * Created by admin on 2017/12/11.
  */
case class CountryCarrierConfig (@BeanProperty  countryId:      Integer,
                                 @BeanProperty  carrierId:      Integer,
                                 @BeanProperty  percent:        Integer)
 extends JSONSerializable{

}
