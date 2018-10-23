package com.mobikok.ssp.data.streaming.entity

import com.fasterxml.jackson.annotation.{JsonIgnore}
import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable
import com.mobikok.ssp.data.streaming.util.OM

import scala.beans.BeanProperty

/**
  * Created by admin on 2017/12/11.
  */
class BidPriceList extends JSONSerializable{

  @BeanProperty  var id:            Integer = _
  @JsonIgnore
  @BeanProperty  var carrierId:     Integer = _
  @JsonIgnore
  @BeanProperty  var countryId:     Integer = _
  @BeanProperty  var bidPrice:      java.math.BigDecimal = _

  def this(id: Integer, carrierId: Integer, countryId: Integer, bidPrice: java.math.BigDecimal) {
    this()
    this.id = id
    this.carrierId = carrierId
    this.countryId = countryId
    this.bidPrice = bidPrice

  }

}


object x{
  def main(args: Array[String]): Unit = {
   println( OM.toJOSN(new BidPriceList(1,1,1,new java.math.BigDecimal(3.3))))
  }
}