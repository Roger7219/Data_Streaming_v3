package com.mobikok.ssp.data.streaming.util

import java.sql.{Connection, ResultSet, Statement}
import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.exception.GreenplumJDBCClientException
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.pivotal.jdbc.GreenplumDriver
import org.apache.commons.dbcp.BasicDataSource


/**
  * Created by Administrator on 2017/6/14.
  */
class GreenplumJDBCClient (moduleName: String, var url: String, var user: String, var password: String) {
  private val LOG = new Logger(moduleName, getClass.getName, new Date().getTime) //LoggerFactory.getLogger(getClass)

  private val driver = classOf[org.postgresql.Driver].getName
  private var basicDataSource: BasicDataSource = null

  try
    Class.forName(driver)
  catch {
    case e: Exception =>
      throw new GreenplumJDBCClientException("加载Greenplum Driver类异常", e)
  }
  basicDataSource = new BasicDataSource
  basicDataSource.setMaxActive(20)
  basicDataSource.setDriverClassName(driver)
  basicDataSource.setUrl(this.url)
  basicDataSource.setRemoveAbandoned(true)
  basicDataSource.setRemoveAbandonedTimeout(180)
  basicDataSource.setUsername(user);
  basicDataSource.setPassword(password);

  def executeBatch (sqls: Array[String]): Unit = {
    LOG.warn("Executing batch SQLs count", sqls.length)

    if (sqls.length == 0) {
      LOG.warn("Executed batch SQLs done", "No statement has been executed !!")
      return
    }

    val conn = basicDataSource.getConnection
    val ac = conn.getAutoCommit
    var st: Statement = null

    try {
      conn.setAutoCommit(false)

      st = conn.createStatement
      sqls.foreach { x =>
        st.addBatch(x)
      }

      LOG.warn("Executing batch SQLs take(2)", util.Arrays.deepToString(sqls.take(2).asInstanceOf[Array[Object]]))

      st.executeBatch()

      conn.commit()

      LOG.warn("Execute batch SQLs done")

    } finally {
      if(ac != null && conn != null) {
        try {
          conn.setAutoCommit(ac)
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

      if (st != null) {
        try {
          st.close()
        }catch {case  e:Exception=>
          e.printStackTrace()
        }
      }

      if(conn != null) {
        try {
        conn.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

    }
  }

  def execute (sql: String): Unit = {
    execute0(sql)
  }

  def executeQuery[T] (sql: String, callback:Callback[T]): T = executeQuery0[T](sql, callback)

  def executeQuery0[T] (sql: String, callback:Callback[T]): T = {
    val s = new Date().getTime
    var conn:Connection = null
    var st: Statement = null
    var rs: ResultSet = null
    try {
      //LOG.info("Executing SQL: " + sql)
      conn = basicDataSource.getConnection
      conn.setAutoCommit(true)
      st = conn.createStatement
      rs = st.executeQuery(sql)
      callback.onCallback(rs)
    } catch {
      case e: Exception =>
        throw new GreenplumJDBCClientException("Fail SQL: " + sql + "\nMessage: "+ e.getMessage, e)
    } finally {

      if(rs != null) {
        try {
          rs.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

      if(st != null) {
        try {
          st.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

      if(conn != null) {
        try {
          conn.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

      LOG.warn("Executed SQL", sql)
    }
  }

  private def execute0 (sql: String) = {
    val s = new Date().getTime
    var conn: Connection = null
    var st: Statement = null

    try {
      //LOG.info("Executing SQL: " + sql)
      conn = basicDataSource.getConnection
      conn.setAutoCommit(true)
      st = conn.createStatement
      st.execute(sql)
    } catch {
      case e: Exception =>
        throw new GreenplumJDBCClientException("Fail SQL: " + sql + "\nMessage: " + e.getMessage, e)
    } finally {
      if(st != null) {
        try {
          st.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }

      if(conn != null) {
        try {
          conn.close()
        }catch {case e:Exception=>
          e.printStackTrace()
        }
      }
      LOG.warn("Executed SQL", sql)
    }
  }
}




