package com.mobikok.ssp.data.streaming.client

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie

/**
  * Created by Administrator on 2017/6/8.
  */
trait Transactional {

  def init() : Unit
  def commit(cookie: TransactionCookie) : Unit
  def rollback(cookies: TransactionCookie*) : Cleanable
  def clean(cookies: TransactionCookie*): Unit

}
