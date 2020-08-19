package com.mobikok.ssp.data.streaming.transaction

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * 对事务操作的返回值的封装，记录了该事务的一些参数
  */
class TransactionCookie(_parentId: String, _id: String) extends JSONSerializable {

  @BeanProperty var parentId = _parentId
  @BeanProperty var id = _id

}