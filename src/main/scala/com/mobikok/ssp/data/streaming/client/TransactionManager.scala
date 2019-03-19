package com.mobikok.ssp.data.streaming.client

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.TransactionManagerException
import com.mobikok.ssp.data.streaming.util.{CSTTime, DateFormatUtil, MySqlJDBCClient, MySqlJDBCClientV2}
import com.typesafe.config.Config
import org.apache.spark.sql.Row
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest

/**
  * Created by Administrator on 2017/6/20.
  */
class TransactionManager(config: Config) {
  def prevRunningSameTransactionGroupModules(moduleName: String) :  Array[String] = {
    mySqlJDBCClientV2.executeQuery(
      s"""
         | select module_name
         | from rollbackable_transaction_cookie
         | where group_name = (
         |   select group_name
         |   from rollbackable_transaction_cookie
         |   where module_name = "$moduleName"
         | )
       """.stripMargin,
      new Callback[Array[String]] {
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

  private val table = config.getString("rdb.transaction.manager.table")

  @volatile private var prevLTime: String = null;

  @deprecated
  private val mySqlJDBCClient = new MySqlJDBCClient(
    config.getString(s"rdb.url"),
    config.getString(s"rdb.user"),
    config.getString(s"rdb.password")
  )

  protected val mySqlJDBCClientV2 = new MySqlJDBCClientV2(
    "",
    config.getString(s"rdb.url"),
    config.getString(s"rdb.user"),
    config.getString(s"rdb.password")
  )

  def isActiveButNotAllCommited (transactionParentId: String): Boolean ={
    mySqlJDBCClientV2.executeQuery(
      s"""
         | select is_all_commited
         | from rollbackable_transaction_cookie
         | where transaction_parent_id = "$transactionParentId"
         |
       """.stripMargin,
      new Callback[Boolean] {
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

  @deprecated
  def isCommited (parentTransactionId: String): Boolean = {
    val rs = mySqlJDBCClient.executeQuery(
      s"""
         | select is_all_commited
         | from $table
         | where transaction_id = "$parentTransactionId"
         |
       """.stripMargin)


    var res = false

    if(rs.next()){
      if("Y".equals(rs.getString("is_all_commited"))) {
        res = true
      }
    }

    try { rs.close() }catch {case e:Exception=>}

//    if(rs.next()) {
//      throw  new TransactionManagerException("The query gets multiple transaction rows, but only one record is allowed to exist")
//    }

    return res
  }

  @volatile private var currBatchTransactionIdIncrCache = new util.HashMap[String, Integer]()

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

//  def strategyInitBatch (dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]]): Unit ={}
//
//  def strategyNeedTransactionalAction ():Boolean = true

//    def initBatch (dwiHivePartitionParts: Array[Array[HivePartitionPart]], dwrHivePartitionParts: Array[Array[HivePartitionPart]]): Unit ={}

  def needTransactionalAction ():Boolean = true


//  def clean (parentTransactionId: String): Unit = {
//    mySqlJDBCClient.execute(s"""delete from $table where transaction_id = "$parentTransactionId" """)
//  }

  private val tidTimeFormat = CSTTime.formatter("yyyyMMdd_HHmmss_SSS") //DateFormatUtil.CST("yyyyMMdd_HHmmss_SSS");// new SimpleDateFormat("yyyyMMdd_HHmmss_SSS")

  def beginTransaction(moduleName: String): String = {
    this.beginTransaction(moduleName, moduleName)
  }

  /**
    * @return transactionParentId
    */
  def beginTransaction(moduleName: String, groupName: String): String = {
    beginTransaction(moduleName, groupName, 0)
  }

  def beginTransaction(moduleName: String, groupName: String, order: Long): String = {
    newTransactionParentId()
  }

  protected def newTransactionParentId():String={
    TransactionManager.syncLock.synchronized{
      Thread.sleep(10)
      return tidTimeFormat.format(new Date)
    }
  }

  private val transactionOrderTimeFormat = CSTTime.formatter("yyyyMMddHHmmssSSS")

  protected def newTransactionOrder(moduleName: String): java.lang.Long = {
    TransactionManager.syncLock.synchronized{
      Thread.sleep(10)
      return java.lang.Long.valueOf(transactionOrderTimeFormat.format(new Date))
    }
  }

  protected def commitTransaction0(isMasterModule: Boolean, parentTransactionId: String, moduleName: String) = {
    currBatchTransactionIdIncrCache.clear()
    mySqlJDBCClient.execute(
      s"""
         | insert into $table (
         |   transaction_id,
         |   is_all_commited
         | )
         | values(
         |   "$parentTransactionId",
         |   "Y"
         | )
       """.stripMargin )
  }

  def clean(moduleName: String) = {
    mySqlJDBCClientV2.execute(s"delete from rollbackable_transaction_cookie where module_name = '$moduleName'")
  }

}

object TransactionManager {
  val parentTransactionIdSeparator = "__"
  val syncLock = new Object
}
