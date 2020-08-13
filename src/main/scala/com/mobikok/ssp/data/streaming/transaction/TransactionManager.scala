package com.mobikok.ssp.data.streaming.transaction

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList, CountDownLatch}
import java.util.{Collections, Date}

import com.mobikok.ssp.data.streaming.client.HiveClient
import com.mobikok.ssp.data.streaming.util.{CSTTime, Logger, ModuleTracer, MySqlJDBCClient}
import com.typesafe.config.Config

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/6/20.
  */
// 多模块共享tid
class TransactionManager(config: Config, transactionalStrategy: TransactionalStrategy) {

  private val LOG: Logger = new Logger("", getClass.getName, new Date().getTime)
//  val TRANSACTION_ACTION_STATUS_READY = 0
//  val TRANSACTION_ACTION_STATUS_BEGINED = 1
//  val TRANSACTION_ACTION_STATUS_COMMITED = 2

//  @volatile private var transactionActionStatus = TRANSACTION_ACTION_STATUS_READY
  @volatile private var transactionParentIdCache: String = null

  @volatile private var LOCK = new Object
  @volatile private var STRATEGY_LOCK = new Object

  //  @volatile private var moduleNames :java.util.HashSet[String] = new util.HashSet[String]()
  @volatile private var countDownLatch: CountDownLatch = null

  @volatile private var isAllModuleTransactionCommited:Boolean = true

  @volatile private var moudleTransactionOrders = new ConcurrentHashMap[String,util.List[Long]]()
  @volatile private var moduleCurrentTransactionOrder  = new ConcurrentHashMap[String, java.lang.Long]()
  @volatile private var mixModuleNames = new util.ArrayList[String]()
  @volatile private var currBatchTransactionIdIncrCache = new util.HashMap[String, Integer]()
  @volatile private var readyCommitModules = new java.util.HashSet[String]()
  @volatile private var uninitializedCurrentBatch = true

  private val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS") //DateFormatUtil.CST("yyyyMMdd_HHmmss_SSS");// new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")
  private val transactionOrderTimeFormat = CSTTime.formatter("yyyyMMddHHmmssSSS")

  protected val mySqlJDBCClient = new MySqlJDBCClient(
    config.getString(s"rdb.url"),
    config.getString(s"rdb.user"),
    config.getString(s"rdb.password")
  )

  ddl()

  private def ddl(): Unit ={
    mySqlJDBCClient.execute(
      """
        |CREATE TABLE IF NOT EXISTS `rollbackable_transaction_cookie`  (
        |  `module_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
        |  `transaction_parent_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Partent transaction id',
        |  `is_all_commited` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Y/N',
        |  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        |  `group_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
        |  PRIMARY KEY (`module_name`) USING BTREE
        |) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
        |
      """.stripMargin)
  }

  def generateTransactionOrder(moduleName: String): java.lang.Long ={
    return newTransactionOrder(moduleName)
  }

  //dwi l_time
  def dwiLTime(moduleConfig: Config):String ={
    if(moduleConfig.hasPath("overwrite") && moduleConfig.getBoolean("overwrite")) HiveClient.OVERWIRTE_FIXED_L_TIME else transactionalStrategy.dwiLTime()
  }

  //dwr l_time
  def dwrLTime(moduleConfig: Config):String={
    if(moduleConfig.hasPath("overwrite") && moduleConfig.getBoolean("overwrite")) HiveClient.OVERWIRTE_FIXED_L_TIME else transactionalStrategy.dwrTime()
  }

  // dwi l_time format
  def dwiLTimeDateFormat(): SimpleDateFormat = transactionalStrategy.dwiLTimeDateFormat
  // dwr l_time format
  def dwrLTimeDateFormat(): SimpleDateFormat = transactionalStrategy.dwrLTimeDateFormat

  // 当前批次是否属于真正的事务操作
  def needRealTransactionalAction():Boolean = {
    val res = transactionalStrategy.needRealTransactionalAction()
    LOG.warn("get needRealTransactionalAction", res )
    res
  }

  def prevRunningSameTransactionGroupModules(moduleName: String) :  Array[String] = {
    mySqlJDBCClient.executeQuery(
      s"""
         | select module_name
         | from rollbackable_transaction_cookie
         | where group_name = (
         |   select group_name
         |   from rollbackable_transaction_cookie
         |   where module_name = "$moduleName"
         | )
       """.stripMargin,
      new com.mobikok.ssp.data.streaming.util.MySqlJDBCClient.Callback[Array[String]] {
        override def onCallback(rs: ResultSet): Array[String] ={
          var res: util.ArrayList[String] = new util.ArrayList[String]()
          while(rs.next()) {
            res.add(rs.getString("module_name"))
          }
          res.toArray[String](new Array[String](0))
        }
      }
    )
  }

  def generateTransactionId(transactionParentId: String): String ={
    var subId: Integer = null
    currBatchTransactionIdIncrCache.synchronized{
      var lastId = currBatchTransactionIdIncrCache.get(transactionParentId)
      if(lastId == null) {
        subId = 0
      }else {
        subId = lastId + 1
      }
      currBatchTransactionIdIncrCache.put(transactionParentId, subId)
    }
    transactionParentId + TransactionManager.parentTransactionIdSeparator + subId
  }

//  override def beginTransaction(moduleName: String): String = {
//    beginTransaction(moduleName, moduleName)
//  }

  // 事务是否 还未结束，即曾调用了beginTransaction()，但还未调用commitTransaction()方法
  def isUncompletedTransaction(moduleName: String): Boolean ={
    moduleCurrentTransactionOrder.containsKey(moduleName)
  }

  def isActiveButNotAllCommitted(transactionParentId: String): Boolean ={
    mySqlJDBCClient.executeQuery(
      s"""
         | select is_all_commited
         | from rollbackable_transaction_cookie
         | where transaction_parent_id = "$transactionParentId"
         |
       """.stripMargin,
      new com.mobikok.ssp.data.streaming.util.MySqlJDBCClient.Callback[Boolean] {
        override def onCallback(rs: ResultSet): Boolean ={
          var res: Boolean = false
          //同一app下的任一模块没有完整提交，则这app下的模块都将回滚
          while(rs.next()){
            if("N".equals(rs.getString("is_all_commited")) ) {
              res = true
            }
          }
          res
        }
      }
    )
  }

//  override def beginTransaction(moduleName: String, groupName: String): String = {
//    beginTransaction(moduleName, groupName, 0)
//  }

  private def addWaitingQueueOrder(moduleName: String, order: Long) {
    var os = moudleTransactionOrders.get(moduleName)
    if(os == null) {
      os = new CopyOnWriteArrayList[Long]()
      moudleTransactionOrders.put(moduleName, os)
    }
    os.add(order)
  }

  private def isMinimum(moduleName: String, order: Long): Boolean ={
    moudleTransactionOrders.get(moduleName).min == order
  }

  // 加入等待队列
  private def waitingQueue(moduleName: String, order: Long): Unit ={

    LOG.warn(" Require begin transaction", "module", moduleName, "ownOrder", order, "currentTransactionOrder", moduleCurrentTransactionOrder.get(moduleName))
    var runnable: Boolean = false
    addWaitingQueueOrder(moduleName, order)
    var lastLogTime = System.currentTimeMillis()
    while(!runnable) {
      synchronizedCall(new Callback {
        override def onCallback(): Unit = {
          runnable = isMinimum(moduleName, order)
        }
      }, LOCK)
      if(runnable) {
        moduleCurrentTransactionOrder.put(moduleName, order)
      }else {
        if(System.currentTimeMillis() - lastLogTime > 1000*30) {
          LOG.warn("Waiting last transaction commit", "module", moduleName, "ownOrder", order,"currentTransactionOrder", moduleCurrentTransactionOrder.get(moduleName), "orders", moudleTransactionOrders.get(moduleName))
          lastLogTime = System.currentTimeMillis()
        }
        Thread.sleep(100)
      }
    }
  }

  def beginTransaction(moduleName: String, groupName: String, order: Long): String = {

    waitingQueue(moduleName, order)

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        //        isAllModuleTransactionCommited = false
        //        moduleTransactionRunningMap.put(moduleName, true)
        LOG.warn(" Obtain begin transaction", "module", moduleName, "order", order, "uninitializedCurrentBatch", uninitializedCurrentBatch)

        if(uninitializedCurrentBatch/*transactionActionStatus == TRANSACTION_ACTION_STATUS_READY || transactionActionStatus == TRANSACTION_ACTION_STATUS_COMMITED*/ /*transactionParentIdCache == null*/) {
          // Key code !!
          transactionalStrategy.initBatch()
          uninitializedCurrentBatch = false
          isAllModuleTransactionCommited = false
          countDownLatch = new CountDownLatch(mixModuleNames.size())

          //          //效验状态
          //          moduleNamesMappingCurrBatchModuleCommitReadied.entrySet().foreach{x=>
          //            if(x.getValue) {
          //              throw new ModuleException("The current module is ready to commit, So cannot start a new transaction")
          //            }
          //          }

          transactionParentIdCache = newTransactionParentId()
          //          countDownLatch = new CountDownLatch(mixModuleNames.size())
          LOG.warn("Generate new transaction id", transactionParentIdCache)

        }else {
          // 读写同一个dwr表的模块们共享一个transaction id
          LOG.warn("Use mix module previous generated transaction parent id", transactionParentIdCache)
        }

      }
    }, LOCK)

    if(transactionalStrategy.needRealTransactionalAction()) {
      //is_all_commited更新为N, transaction_parent_id更新为当前的parentId（可回滚事务的）
      mySqlJDBCClient.execute(
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
      mySqlJDBCClient.execute(
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

  def commitTransaction(isMasterModule: Boolean, moduleName:String, moduleTracer: ModuleTracer, commitCallback: =>Unit): Unit ={
    LOG.warn(s"Wait mix module transaction commit [start]", "moduleName", moduleName, "isMaster", isMasterModule, "transactionParentId", getCurrentTransactionParentId())
    moduleTracer.trace("wait mix tx commit start")

    readyCommitModules.add(moduleName)

    countDownLatch.countDown()

    countDownLatch.await()

    commitTransaction0(isMasterModule, getCurrentTransactionParentId(), moduleName)
    commitCallback

    if(isMasterModule) {
      isAllModuleTransactionCommited = true
      // reset
      readyCommitModules.clear()
      countDownLatch = null
      uninitializedCurrentBatch = true
    } else {
      // Wait master module reset
      while(!isAllModuleTransactionCommited) {
        Thread.sleep(100)
      }
    }

    moudleTransactionOrders.get(moduleName).remove(moduleCurrentTransactionOrder.get(moduleName))
    moduleCurrentTransactionOrder.remove(moduleName)

    LOG.warn(s"Wait mix module transaction commit [done]", "moduleName", moduleName, "isMaster", isMasterModule, "transactionParentId", getCurrentTransactionParentId())
    moduleTracer.trace("wait mix tx commit done")
  }

  private def commitTransaction0(isMasterModule: Boolean, parentTransactionId: String, moduleName: String) = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {

        if(isMasterModule) {

          if(transactionalStrategy.needRealTransactionalAction()) {
            mySqlJDBCClient.execute(
              s"""
                 | update rollbackable_transaction_cookie
                 | set is_all_commited = "Y"
                 | where transaction_parent_id = "$parentTransactionId"
            """.stripMargin)

//            transactionActionStatus = TRANSACTION_ACTION_STATUS_COMMITED
          }

        }
      }
    }, LOCK)

    //当前批次没有做可回滚操作
    if(!transactionalStrategy.needRealTransactionalAction()) {
      //将该模块最近一次可回滚事务的状态重新更新为Y，表示所有操作都已完整提交，下次重启无需回滚
      mySqlJDBCClient.execute(
        s"""
           | update rollbackable_transaction_cookie
           | set is_all_commited = "Y"
           | where module_name = "${moduleName}"
              """.stripMargin
      )
    }
  }

  //初始化app的时候调用
  def addModuleName(moduleName: String): Unit = {
    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        mixModuleNames.add(moduleName)
        //        currentBatchTransactionReadyCommits.put(moduleName, false)
        //        countDownLatch = new CountDownLatch(moduleNames.size())
      }
    }, LOCK)
    LOG.warn("Added module", "name", moduleName, "mixModuleNames", mixModuleNames)
  }

  private def synchronizedCall(callback: Callback, lock: Object): Unit ={
    lock.synchronized{
      callback.onCallback()
    }
  }


  def newTransactionParentId():String={
    TransactionManager.syncLock.synchronized{
      Thread.sleep(10)
      return tidTimeFormat.format(new Date)
    }
  }

  protected def newTransactionOrder(moduleName: String): java.lang.Long = {
    TransactionManager.syncLock.synchronized{
      Thread.sleep(10)
      return java.lang.Long.valueOf(transactionOrderTimeFormat.format(new Date))
    }
  }

  def deleteTransactionCookie(moduleName: String) = {
    mySqlJDBCClient.execute(s"delete from rollbackable_transaction_cookie where module_name = '$moduleName'")
  }

  var batchesTransactionCookiesCache = new util.HashMap[TransactionalClient, util.ArrayList[TransactionCookie]]()

  def collectTransactionCookie(transactionalClient: TransactionalClient, transactionCookie: TransactionCookie): Unit ={
    collectTransactionCookies(transactionalClient, Collections.singletonList(transactionCookie))
  }

  def collectTransactionCookies(transactionalClient: TransactionalClient, transactionCookies: util.List[TransactionCookie]): Unit ={
    transactionalClient.synchronized{
      var pr = batchesTransactionCookiesCache.get(transactionalClient)
      if (pr == null) {
        pr = new util.ArrayList[TransactionCookie]()
        batchesTransactionCookiesCache.put(transactionalClient, pr)
      }
      pr.addAll(transactionCookies)
    }
  }


  // 调用这个方法时，说明当前事务已经commit好了，事务数据已经准备好了，上一次的事务数据就不再需要了
  // 清理上一个事务的，要排除当前事务，当前事务的不能清理，若清理了就无法回滚了！
  def cleanLastTransactionCookie(transactionalClient: TransactionalClient): Unit ={
      transactionalClient.clean(popNeedCleanTransactions(transactionalClient):_*)
  }

  // 取上一个事务的，要排除当前事务，因为当前事务的不能清理，如果清理了就无法回滚了！
  private def popNeedCleanTransactions(transactionalClient: TransactionalClient): Array[TransactionCookie] = {
    var result = Array[TransactionCookie]()
    val cs = batchesTransactionCookiesCache.get(transactionalClient)
    if (needRealTransactionalAction() && cs != null) {
      // 排除当前事务父id，取以前的
      val excludeCurrent = getCurrentTransactionParentId()
      val cleanCookies = cs.filter(!_.parentId.equals(excludeCurrent))
      cs.removeAll(cleanCookies)
      result = cleanCookies.toArray
    }
    result
  }

  trait Callback{
    def onCallback()
  }

}

object TransactionManager {
  val parentTransactionIdSeparator = "__"
  val syncLock = new Object
}