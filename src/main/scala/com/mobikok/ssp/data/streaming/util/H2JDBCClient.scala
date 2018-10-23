package com.mobikok.ssp.data.streaming.util

import java.sql.{Connection, DriverManager}

import org.apache.log4j.Logger
import org.h2.Driver

class H2JDBCClient(url: String, username: String, password: String) {

  private[this] val LOG = Logger.getLogger(getClass.getName)

  classOf[Driver]
  var conn: Connection = _
  var retryTimes = 3

  def execute(sql: String): Unit = {
    execute0(sql, retryTimes)
  }

  private def execute0(sql: String, _retryTimes: Int): Unit = {
    try {
      if (conn == null) {
        conn = DriverManager.getConnection(url, username, password)
      }
      conn.createStatement().execute(sql)
    } catch {
      case e: Exception =>
        if (_retryTimes > 0) {
          if (conn == null) {
            conn = DriverManager.getConnection(url, username, password)
          }
          execute0(sql, _retryTimes - 1)
        }
        throw new RuntimeException(s"Trying ${_retryTimes+1} attempts to perform the hive JDBC operation still failed: ", e)
    } finally {
      if (conn != null) {
        conn.commit()
        conn.close()
        conn = null
      }
    }
  }

  def executeBatch(sqls: String*): Unit = {
    executeBatch0(sqls, retryTimes)
  }

  private def executeBatch0(sqls: Seq[String], _retryTimes: Int): Unit = {
    try {
      if (conn == null) {
        conn = DriverManager.getConnection(url, username, password)
      }
      val statement = conn.createStatement()
      for (sql <- sqls) {
        statement.addBatch(sql)
      }
      statement.executeBatch()
    } catch {
      case e: Exception =>
        if (_retryTimes > 0) {
          if (conn == null) {
            conn = DriverManager.getConnection(url, username, password)
          }
          conn.rollback()
          executeBatch0(sqls, _retryTimes - 1)
        }
        throw new RuntimeException(s"Trying ${_retryTimes+1} attempts to perform the hive JDBC operation still failed: ", e)
    } finally {
      if (conn != null) {
        conn.commit()
        conn.close()
        conn = null
      }
    }
  }

}

object H2JDBCClient {

  def main(args: Array[String]): Unit = {
//    val h2JDBCClient = new H2JDBCClient("jdbc:h2:tcp://192.168.34.129:9999/mem:campaign;DB_CLOSE_DELAY=-1", "", "")
    classOf[Driver]
    val conn = DriverManager.getConnection("jdbc:h2:tcp://node14:10010/mem:campaign;DB_CLOSE_DELAY=-1", "", "")
    val resultSet = conn.createStatement().executeQuery("SELECT count(1) FROM CAMPAIGN_SEARCH;")
//    val resultSet = conn.createStatement().executeQuery("show tables;")
    while (resultSet.next()) {
      println(resultSet.getString(1))
    }
  }
}
