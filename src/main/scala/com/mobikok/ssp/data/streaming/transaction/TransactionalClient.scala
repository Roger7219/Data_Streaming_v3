package com.mobikok.ssp.data.streaming.transaction

/**
  * 对数据库（或数据仓库）操作需要支持事务的客户端，需要继承该类，
  * 比如重要实现类有：HiveClient，KafkaClient！
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
