package com.mobikok.ssp.data.streaming.client.cookie

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable

import scala.beans.BeanProperty

/**
  * Created by Administrator on 2017/6/8.
  */
class TransactionCookie(_parentId: String, _id: String) extends JSONSerializable {

  @BeanProperty var parentId = _parentId
  @BeanProperty var id = _id

}