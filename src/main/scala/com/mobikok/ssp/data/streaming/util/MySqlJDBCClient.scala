package com.mobikok.ssp.data.streaming.util

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.Comparator

import com.mobikok.ssp.data.streaming.exception.{HiveJDBCClientException, MySQLJDBCClientException}
import com.mysql.jdbc.Driver
import org.apache.hive.jdbc.HiveDriver
import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/6/14.
  */

class MySqlJDBCClient (url: String, user: String, password: String) {

  private[this] val LOG = Logger.getLogger(getClass().getName());

  classOf[Driver]
  var conn: Connection = null

  val retryTimes = 0

  def execute (sql: String): Unit = {
    var b = true
    while (b) {
      try{
        execute0(sql, retryTimes)
        b = false
      }catch {case e:Throwable=>
        LOG.warn("MySql execute fail, Will try again !!", e)
        Thread.sleep(10000)
      }
    }
  }

  def executeQuery (sql: String) : ResultSet = {
    var res:Any = null
    var b = true
    while (b) {
      try{
        res = executeQuery0(sql, retryTimes)
        b = false
      }catch {case e:Throwable=>
        LOG.warn("MySql execute query fail, Will try again !!", e)
        Thread.sleep(5000)
      }
    }
    res.asInstanceOf[ResultSet]
  }

  private def executeQuery0 (sql: String, _retryTimes: Int) : ResultSet = {
    try {
      if(conn == null) {
        conn = DriverManager.getConnection(url, user, password)
      }

      LOG.info(s"Executing SQL: $sql" )
      return conn.createStatement.executeQuery(sql)
    } catch {
      case e: Exception =>

//        if (_retryTimes > 0) {
          // Re initialization Connection
          conn = DriverManager.getConnection(url, user, password)
//          val res = executeQuery0(sql, (_retryTimes - 1))
//          LOG.info(s"Executed SQL: $sql" )
//          return res
//        }
        if (conn != null) {
          try {
            conn.close()
          }finally {
            conn = null
          }
        }
        throw new MySQLJDBCClientException(s"MySql execute query fail:\n\n        SQL: " + sql +"\n", e)
    }
  }

  private def execute0 (sql: String, _retryTimes: Int): Unit = {
    try {
      if(conn ==null) {
        conn = DriverManager.getConnection(url, user, password)
      }

      LOG.info(s"Executing SQL: $sql" )
      conn.createStatement.execute(sql)
    } catch {
      case e: Exception =>

//        if (_retryTimes > 0) {
//          // Re initialization Connection
          conn = DriverManager.getConnection(url, user, password)
//          execute0(sql, (_retryTimes - 1))
//          LOG.info(s"Executed SQL: $sql" )
//          return
//        }
        if (conn != null) {
          try {
            conn.close()
          }finally {
            conn = null
          }
        }
        throw new MySQLJDBCClientException(s"MySql execute fail::\n\n        SQL: " + sql +"\n", e)
    }
  }


}
//object x{
//  def main (args: Array[String]): Unit = {
//    val url = "jdbc:mysql://node17:3306/sight"
//    val user = "root"
//    val password = "root_root"
//
//    val mySqlJDBCClient = new MySqlJDBCClient(
//      url,
//      user,
//      password
//    )
//
//   println(readRDBConfig("streaming_status"))
//
//    def readRDBConfig (name: String): String = {
//
//      val rs = mySqlJDBCClient.executeQuery("""select value from config where name = "streaming_status" """)
//      if(rs.next()){
//        rs.getString(1)
//      }else
//      null
//    }
//
//  }
//
//}

//object x{
//  private val transactionalLegacyDataBackupCompletedTableSign = "_backup_completed_"
//  private val transactionalLegacyDataBackupProgressingTableSign = "_backup_progressing_"
//  private val backupCompletedTableNameForTransactionalTmpTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign
//  private val backupCompletedTableNameForTransactionalTargetTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign +".*"
//
//
//  private val transactionalTmpTableSign = "_trans_"
//
//
//  def main (args: Array[String] ): Unit = {
////    println("asd_asdqw".replaceAll("_","."))
//
//    val list = new util.ArrayList[String]()
//    list.add("asd")
//    list.add("z")
//    list.add("vrr")
//
//
//    list.sort(new Comparator[String] {
//      override def compare (a: String, b: String): Int = b.compareTo(a)
//    })
//
//    println(list)
//
////    val t ="ssp_user_dwi_uuid_stat_class_com_mobikok_ssp_data_streaming_entity_UuidStat_backup_completed_20170621_092945_684__1"
////    val tt = t.replaceFirst(backupCompletedTableNameForTransactionalTmpTableNameReplacePattern, transactionalTmpTableSign)
////    val table =  t.replaceFirst(backupCompletedTableNameForTransactionalTargetTableNameReplacePattern, "")
////    println(tt)
////    println(table)
//
//
//  }
//}



//
//object x {
//
//  def main (args: Array[String]): Unit = {
//   /* val classPattern = "_class_(.+)".r
//    classPattern.findAllIn("asdad_class_1U_asd_S").matchData.foreach{x=>
//      println(x.group(1).replaceAll("_","."))
//    }
//
//    val string = "one493two483three"
//    val pattern = """two(\d+)three""".r
//    pattern.findAllIn(string).matchData foreach {
//      m => println(m.group(1))
//    }
//*/
//    //    val s= Array("123","we")
////   val s2 = s +: "xx"
////      s2.foreach{x=>
////        println(
////          x
////        )
////
////      }
//
//
////        println("cccccc1")
////        new HiveJDBCClient("jdbc:hive2://node17:10000/default", null, null).execute("create table ss23(data string)")
////        println("cccccc2")
//  }
//}

