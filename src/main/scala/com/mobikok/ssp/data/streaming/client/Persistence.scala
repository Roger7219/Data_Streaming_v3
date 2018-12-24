package com.mobikok.ssp.data.streaming.client

trait Persistence extends Transactional {

  var transactionParentId: String = _
  var cookieKindMark: String = _

  override def init(): Unit = {}

}
