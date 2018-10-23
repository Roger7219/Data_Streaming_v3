package com.mobikok.ssp.data.streaming.util

import java.sql.{Connection, DriverManager, ResultSet}
import java.util

import com.mobikok.ssp.data.streaming.exception.HiveJDBCClientException
import org.apache.hive.jdbc.HiveDriver
import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/6/14.
  */

class HiveJDBCClient (url: String, user: String, password: String) {

  private[this] val LOG = Logger.getLogger(getClass().getName());

  classOf[HiveDriver]
  var conn: Connection = null

  val retryTimes = 3

  def execute (sql: String): Unit = {
    execute0(sql, retryTimes)
  }

  private def execute0 (sql: String, _retryTimes: Int): Unit = {
    try {
      if(conn ==null) {
        conn = DriverManager.getConnection(url, user, password)
      }

      LOG.info(s"Execute SQL: $sql" )
      conn.createStatement.execute(sql)
    } catch {
      case e: Exception =>

        if (_retryTimes > 0) {
          // Re initialization Connection
          conn = DriverManager.getConnection(url, user, password)
          execute0(sql, (_retryTimes - 1))
          return
        }
        if (conn != null)
          try {
            conn.close()
            conn = null
          }
        throw new HiveJDBCClientException(s"Trying ${_retryTimes+1} attempts to perform the hive JDBC operation still failed: ", e)
    }
  }

}
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

