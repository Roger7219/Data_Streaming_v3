package com.mobikok.ssp.data.streaming.module

import java.util.Date

/**
  * Created by Administrator on 2017/6/8.
  */
trait Module {
//  def isMaster(): Boolean
//  def getName(): String
//  def isRunnable(): Boolean
//  def isInitable(): Boolean

  def init() : Unit
//  def start() : Unit
  def handler() : Unit
  def stop() : Unit

}
