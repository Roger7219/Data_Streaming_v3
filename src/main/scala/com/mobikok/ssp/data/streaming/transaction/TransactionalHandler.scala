package com.mobikok.ssp.data.streaming.transaction

/**
  * Created by Administrator on 2017/6/8.
  */
trait TransactionalHandler {

  def commit(): Unit
  def clean(): Unit
}
