package com.mobikok.ssp.data.streaming.entity

import com.fasterxml.jackson.annotation.{JsonIgnore}
import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable
import com.mobikok.ssp.data.streaming.util.OM

import scala.beans.BeanProperty

/**
  * Created by admin on 2017/12/11.
  */
class SoldoutOfferConfig extends JSONSerializable{

  @BeanProperty  var clickCount:            Integer  = _
  @BeanProperty  var ecpc:                  java.math.BigDecimal = _
  @BeanProperty  var cr:                    java.math.BigDecimal = _

  def this(clickCount: Integer , ecpc: java.math.BigDecimal, cr: java.math.BigDecimal) {
    this()
    this.clickCount = clickCount
    this.ecpc = ecpc
    this.cr = cr

  }

}


//object sxc{
//  def main(args: Array[String]): Unit = {
//   println( OM.toJOSN(new SoldoutOfferConfig(20000,0.001,0.1000)))
//  }
//}