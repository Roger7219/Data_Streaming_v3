package com.mobikok.ssp.data.streaming.client

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.{ModuleException, TransactionManagerException}
import com.mobikok.ssp.data.streaming.module.support.{OptimizedTransactionalStrategy, TransactionalStrategy}
import com.mobikok.ssp.data.streaming.util.{Logger, MySqlJDBCClientV2}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback

import scala.collection.JavaConversions._
import com.typesafe.config.Config

/**
  * Created by Administrator on 2017/6/20.
  */
// 多模块共享tid
class MixTransactionManager (config: Config, transactionalStrategy: TransactionalStrategy) extends TransactionManager (config){

  private val LOG: Logger = new Logger("", getClass.getName, new Date().getTime)

  @volatile private var transactionParentIdCache: String = null

  @volatile private var LOCK = new Object
  @volatile private var STRATEGY_LOCK = new Object

//  @volatile private var moduleNames :java.util.HashSet[String] = new util.HashSet[String]()
  @volatile private var countDownLatch: CountDownLatch = null

  @volatile private var moduleNamesMappingCurrBatchModuleCommitReadied: java.util.HashMap[String, Boolean] = new util.HashMap[String, Boolean]()

//  @volatile private var currBatchNeedTransactionalPersistence: Option[Boolean] = None
  @volatile private var currMixBatchInited = false

//  override def strategyInitBatch (dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]]): Unit ={
//    synchronizedCall(new Callback {
//      override def onCallback (): Unit = {
//        if(!currMixBatchInited) {
//          transactionalStrategy.initBatch(dwiHivePartitionParts, dwrHivePartitionParts)
//          currMixBatchInited = true
//        }
//      }
//    }, LOCK)
//
////    if(strategyNeedTransactionalAction()) {
////      mySqlJDBCClientV2.execute(
////        s"""
////           | insert into rollbackable_transaction_cookie(
////           |   module_name,
////           |   transaction_parent_id,
////           |   is_all_commited
////           | )
////           | values(
////           |   "$moduleName",
////           |   "$transactionParentIdCache",
////           |   "N"
////           | )
////           | on duplicate key update
////           |   transaction_parent_id = values(transaction_parent_id),
////           |   is_all_commited = "N"
////           |
////         """.stripMargin)
////    }
//
//  }

//  override def strategyNeedTransactionalAction (): Boolean = {
////    synchronizedCall(new Callback {
////      override def onCallback (): Unit = {
////        if(currBatchNeedTransactionalPersistence == None) {
////          currBatchNeedTransactionalPersistence = Option(transactionalStrategy.needTransactionalAction())
////        }
////      }
////    }, LOCK)
////
////    var res = currBatchNeedTransactionalPersistence.get
//
//    if(!currMixBatchInited) {
//      throw new TransactionManagerException("Current batch strategy uninitialized, Need call strategyInitBatch() first!")
//    }
//
//    var res = transactionalStrategy.needTransactionalAction()
//    LOG.warn("get needTransactionalAction", res )
//    res
//  }

  def dwiLoadTime():String ={
    transactionalStrategy.dwiLoadTime()
  }

  def dwrLoadTime():String={
    transactionalStrategy.dwrLoadTime()
  }

  def dwiLoadTimeDateFormat(): SimpleDateFormat = transactionalStrategy.dwiLoadTimeDateFormat
  def dwrLoadTimeDateFormat(): SimpleDateFormat = transactionalStrategy.dwrLoadTimeDateFormat

  override def needTransactionalAction ():Boolean = {
    var res = transactionalStrategy.needTransactionalAction()
    LOG.warn("get needTransactionalAction", res )
    res
  }

  override def beginTransaction(moduleName: String): String = {
    beginTransaction(moduleName, moduleName)
  }

  private def waitLastTransactionCommit(moduleName: String, groupName: String): Unit ={
    // 验证是否已clean
    while(mySqlJDBCClientV2.executeQuery(
      s"""
         | select
         |  is_all_commited
         | from rollbackable_transaction_cookie
         | where transaction_parent_id = (
         |   select transaction_parent_id
         |   from rollbackable_transaction_cookie
         |   where module_name = "$moduleName"
         | )
      """.stripMargin, new MySqlJDBCClientV2.Callback[Boolean](){
        override def onCallback(rs: ResultSet): Boolean = {
          var res = false
          if(rs.next()){
            if("N".equals(rs.getString("is_all_commited"))) {
              res = true
            }
          }
          res
        }
      })) {

      Thread.sleep(1000*2)
    }
  }

  override def beginTransaction(moduleName: String, groupName: String): String = {

//    waitLastTransactionCommit(moduleName, groupName)

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        if(transactionParentIdCache == null) {

          // Key code !!
          transactionalStrategy.initBatch()

          //效验状态
          moduleNamesMappingCurrBatchModuleCommitReadied.entrySet().foreach{x=>
            if(x.getValue) {
              throw new ModuleException("The current module is ready to commit, So cannot start a new transaction")
            }
          }

          //仅为了兼容历史模块GenericModule
          transactionParentIdCache = MixTransactionManager.super.beginTransaction(moduleName, groupName)

          countDownLatch = new CountDownLatch(moduleNamesMappingCurrBatchModuleCommitReadied.size())

          LOG.warn("Generate new transaction id", transactionParentIdCache)

        }else {
          // 读写同一个dwr表的模块们共享一个transaction id
          LOG.warn("Use previous generated transaction id", transactionParentIdCache)
        }
      }
    }, LOCK)

    if(transactionalStrategy.needTransactionalAction()) {
      //is_all_commited更新为N, transaction_parent_id更新为当前的可回滚事务的parentId
      mySqlJDBCClientV2.execute(
        s"""
           | insert into rollbackable_transaction_cookie(
           |   module_name,
           |   transaction_parent_id,
           |   is_all_commited,
           |   group_name
           | )
           | values(
           |   "$moduleName",
           |   "$transactionParentIdCache",
           |   "N",
           |   "$groupName"
           | )
           | on duplicate key update
           |   transaction_parent_id = values(transaction_parent_id),
           |   is_all_commited = "N",
           |   group_name = "$groupName"
              """.stripMargin)
    }else {
      //is_all_commited更新为N, rollbackable transaction_parent_id保持最近一次可回滚事务的parentId不变
      mySqlJDBCClientV2.execute(
        s"""
           | insert into rollbackable_transaction_cookie(
           |   module_name,
           |   is_all_commited,
           |   group_name
           | )
           | values(
           |   "$moduleName",
           |   "N",
           |   "$groupName"
           | )
           | on duplicate key update
           |   transaction_parent_id = transaction_parent_id,
           |   is_all_commited = "N",
           |   group_name = "$groupName"
              """.stripMargin)
    }

    transactionParentIdCache
  }

  def getCurrentTransactionParentId (): String ={
    transactionParentIdCache
  }

  @volatile private var isCommited = false
  def waitAllModuleReadyCommit (isMasterModule: Boolean, moduleName:String, callback: =>Unit): Unit ={
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        moduleNamesMappingCurrBatchModuleCommitReadied.put(moduleName, true)
      }
    }, LOCK)

    isCommited = false

    //Wait for all module readied
    var b = true
    while(b) {
      var allReadied = true
      synchronizedCall(new Callback {
        override def onCallback (): Unit = {
          moduleNamesMappingCurrBatchModuleCommitReadied.entrySet().foreach{x=>
            if(!x.getValue  && allReadied) {
              allReadied = false
            }
          }
        }
      }, LOCK)

      if(allReadied) {
        b = false
        countDownLatch.countDown()
      }else {
        Thread.sleep(100)
      }
    }

    countDownLatch.await()

    callback
    if(isMasterModule) {
      isCommited = true
    } else {
      while(!isCommited) {
        Thread.sleep(500)
      }
    }

//    synchronizedCall(new Callback {
//      override def onCallback (): Unit = {
//        if(countDownLatch != null) {
//          countDownLatch = null
//        }
//      }
//    }, lock)
//
//    synchronizedCall(new Callback {
//      override def onCallback (): Unit = {
//        if(countDownLatch == null) {
//          countDownLatch = new CountDownLatch(moduleNames.size())
//        }
//      }
//    }, lock)

  }

  override def commitTransaction (isMasterModule: Boolean, parentTransactionId: String, moduleName: String) = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        if(isMasterModule) {
          //仅为了兼容历史模块GenericModule
          MixTransactionManager.super.commitTransaction(isMasterModule, parentTransactionId, moduleName)

          if(transactionalStrategy.needTransactionalAction()) {
            mySqlJDBCClientV2.execute(
              s"""
                 | update rollbackable_transaction_cookie
                 | set is_all_commited = "Y"
                 | where transaction_parent_id = "$parentTransactionId"
            """.stripMargin)
          }

          transactionParentIdCache = null
          currMixBatchInited = false
  //        currBatchNeedTransactionalPersistence = None

          moduleNamesMappingCurrBatchModuleCommitReadied.entrySet().foreach{x=>
            moduleNamesMappingCurrBatchModuleCommitReadied.put(x.getKey, false)
          }
  //        countDownLatch = new CountDownLatch(moduleNamesMappingCurrBatchModuleCommitReadied.size())
        }
      }
    }, LOCK)

    //当前批次没有做可回滚操作
    if(!transactionalStrategy.needTransactionalAction()) {
      //将该模块最近一次可回滚事务的状态重新更新为Y，表示所有操作都已完整提交，下次重启无需回滚
      mySqlJDBCClientV2.execute(
        s"""
           | update rollbackable_transaction_cookie
           | set is_all_commited = "Y"
           | where module_name = "${moduleName}"
              """.stripMargin
      )
    }
  }

  //初始化app的时候调用
  def addMoudleName(moudleName: String): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        moduleNamesMappingCurrBatchModuleCommitReadied.put(moudleName, false)
//        countDownLatch = new CountDownLatch(moduleNames.size())
      }
    }, LOCK)
  }

  private def synchronizedCall(callback: Callback, lock: Object): Unit ={
    lock.synchronized{
      callback.onCallback()
    }
  }


  trait Callback{
    def onCallback()
  }

}


//
//object O {
//
//  def main (args: Array[String]): Unit = {
//    println(None == Option(true))
//
//    //    v({
////      println("we")
////    })
//  }
//
//  def v( c: => Unit) {
//    c
//    println("cf")
//  }
//}