package com.mobikok.ssp.data.streaming.transaction

/**
  * dwi/dwr层支持事务的handler需要继承该类
  */
trait TransactionalHandler {

  def commit(): Unit
  def clean(): Unit
}
