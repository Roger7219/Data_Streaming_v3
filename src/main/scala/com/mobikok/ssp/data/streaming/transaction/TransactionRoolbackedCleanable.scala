package com.mobikok.ssp.data.streaming.transaction

import scala.collection.mutable.ListBuffer

/**
  * 封装了对“事务过程中产生的临时数据”的清理操作
  * Created by Administrator on 2018/3/3.
  */
class TransactionRoolbackedCleanable {

  private var actions: ListBuffer[ Unit=>Any] = ListBuffer[Unit=> Any]()

  def addAction(action: =>Any): TransactionRoolbackedCleanable = {
    actions.append({Unit=> action})
    this
  }

  def doActions(): Unit ={
    actions.foreach{x=>
      x.apply()
    }
    actions = ListBuffer[Unit=> Any]()
  }

}