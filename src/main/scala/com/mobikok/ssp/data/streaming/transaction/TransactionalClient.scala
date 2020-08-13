package com.mobikok.ssp.data.streaming.transaction

/**
  * 重要实现类有：HiveClient，KafkaClient！
  *
  * HBaseClient虽然有实现，但性能差，目前不建议用事务方法，
  * 而是用非事务方法HBaseClient.putsNonTransaction()，其能满足大部分需求
  */
trait TransactionalClient {

  def init() : Unit
  def rollback(cookies: TransactionCookie*) : TransactionRoolbackedCleanable
  def commit(cookie: TransactionCookie) : Unit
  def clean(cookies: TransactionCookie*): Unit
}
