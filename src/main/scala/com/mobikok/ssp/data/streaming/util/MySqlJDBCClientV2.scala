package com.mobikok.ssp.data.streaming.util

import org.apache.log4j.Logger
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util
import java.util.Map.Entry
import java.util.{Comparator, Date}

import com.mobikok.ssp.data.streaming.exception.MySQLJDBCClientException
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import org.apache.commons.dbcp.BasicDataSource
import org.datanucleus.store.rdbms.JDBCUtils

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2017/6/14.
  */
class MySqlJDBCClientV2 (moduleName: String, var url: String, var user: String, var password: String) {
  private val LOG = new Logger( getClass.getName, new Date().getTime) //LoggerFactory.getLogger(getClass)

  private val driver = "com.mysql.jdbc.Driver"
  var basicDataSource: BasicDataSource = _

  try
    Class.forName(driver)
  catch {
    case e: Exception =>
      throw new MySQLJDBCClientException("加载Mysql Driver类异常", e)
  }
  basicDataSource = new BasicDataSource
  basicDataSource.setMaxActive(5)
  basicDataSource.setDriverClassName(driver)
  basicDataSource.setUrl(this.url)
  basicDataSource.setRemoveAbandoned(true)
  basicDataSource.setRemoveAbandonedTimeout(180)
  basicDataSource.setUsername(user);
  basicDataSource.setPassword(password);
  basicDataSource.setValidationQuery("select 1");
  basicDataSource.setTestWhileIdle(true)
  //在空闲连接回收器线程运行期间休眠的时间值,以毫秒为单位，一般比minEvictableIdleTimeMillis小
  basicDataSource.setTimeBetweenEvictionRunsMillis(1000*60*5);
  //在每次空闲连接回收器线程(如果有)运行时检查的连接数量，最好和maxActive一致
  basicDataSource.setNumTestsPerEvictionRun(5);
  //连接池中连接，在时间段内一直空闲，被逐出连接池的时间(1000*60*60)，以毫秒为单位
  basicDataSource.setMinEvictableIdleTimeMillis(1000*60*60)

  def executeBatch(sqls: Array[String]): Unit ={
    executeBatch(sqls, 2000)
  }

  def executeBatch(sqls: Array[String], groupSize:Int): Unit ={
    var b = true

    LOG.warn("Executing batch SQLs, All group count", sqls.length)
    val gCount = Math.ceil(1.0*sqls.length / groupSize)

//    while (b) {
//      try{
//          sqls.zipWithIndex.groupBy(x => x._2 % gCount).foreach { y =>
//            val _sqls = y._2.map(z=>z._1)
//
//            executeBatch0(_sqls)
//          }
//          b = false
//      }catch {case e:Throwable=>
//        LOG.error("MySql executeBatch Fail, Will try again !!", e)
//        Thread.sleep(10000)
//      }
//    }

      sqls.zipWithIndex.groupBy(x => x._2 % gCount).foreach { y =>
        RunAgainIfError.run({
          val _sqls = y._2.map(z=>z._1)
          executeBatch0(_sqls)
        }, "MySql execute batch fail, Will try again !!")
      }
  }

  def executeBatch(preparedSql: String, batchData: Map[Int, _]*): Unit ={
    LOG.warn("Executing batch prepared SQL, count", batchData.length)

//    executeBatch("insret into t1(f1, f2) values(?,?)", Map(1->1, 2, "xx") )

    RunAgainIfError.run({

      if (0 == preparedSql.length || 0 == batchData.length) {
        LOG.warn("Nonthing to be executed !!")
        return
      }

      val con = basicDataSource.getConnection
      var ps: PreparedStatement = null

      try {
        con.setAutoCommit(false)

         ps = con.prepareStatement(preparedSql)

        batchData.foreach { x =>
//            var v = new util.ArrayList[Entry[Int, _]](x.entrySet())
//
//            util.Collections.sort(v, new Comparator[Entry[Int, _]](){
//              def compare(a: Entry[Int, _], b: Entry[Int, _]): Int = {
//                a.getKey.compareTo(b.getKey)
//              }
//            })

          x.entrySet().foreach { y =>
//            println(y.getKey +" " + y.getValue)
            ps.setObject(y.getKey, y.getValue)
          }
          ps.addBatch
        }
          ps.executeBatch

          con.commit()

          LOG.warn("Execute batch prepared SQL done")
        } finally {

          if(con != null) {
            try {
              con.close()
            }catch {case e:Exception=>
              e.printStackTrace()
            }
          }

        if(ps != null) {
          try {
            ps.close()
          }catch {case e:Exception=>
            e.printStackTrace()
          }
        }

        }
    })
  }


  def executeBatch(preparedSql: String, groupSize:Int, batchData: Map[Int, _]*): Unit ={
    LOG.warn("Executing batch prepared SQL, count", batchData.length)

    //    executeBatch("insret into t1(f1, f2) values(?,?)", Map(1->1, 2, "xx") )

//    RunAgainIfError.run({

      if (0 == preparedSql.length || 0 == batchData.length) {
        LOG.warn("Nonthing to be executed !!")
        return
      }

      val con = basicDataSource.getConnection
      var ps: PreparedStatement = null

      try {
        con.setAutoCommit(false)

        ps = con.prepareStatement(preparedSql)

        val gCount = Math.ceil(1.0*batchData.length / groupSize)
        LOG.warn("Executing batch SQLs", "count", batchData.length, "groupSize", groupSize, "groupCount", gCount)

          batchData.zipWithIndex.groupBy(x => x._2 % gCount).foreach { y =>
            RunAgainIfError.run({
              val groupData = y._2.map(z=>z._1)

              groupData.foreach { x =>
                x.entrySet().foreach { y =>
                  ps.setObject(y.getKey, y.getValue)
                }
                ps.addBatch
              }

              ps.executeBatch
              con.commit()
            }, "MySql execute batch fail, Will try again !!")
          }

        LOG.warn("Execute batch prepared SQL done")
      } finally {

        if(con != null) {
          try {
            con.close()
          }catch {case e:Exception=>
            e.printStackTrace()
          }
        }

        if(ps != null) {
          try {
            ps.close()
          }catch {case e:Exception=>
            e.printStackTrace()
          }
        }

      }
//    })
  }


  private def executeBatch0 (sqls: Array[String]): Unit = {

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

//    var b = true
//    while (b) {
//      try{
//        execute0(sql)
//        b = false
//      }catch {case e:Throwable=>
//        LOG.error("MySql execute fail, Will try again !!", e)
//        Thread.sleep(5000)
//      }
//    }
    RunAgainIfError.run({
      execute0(sql)
    }, "MySql execute fail, Will try again !!")
  }

  def executeQuery[T] (sql: String, callback:Callback[T]): T = {
    var res:Any = null
    var b = true
    while (b) {
      try{
        res = executeQuery0[T](sql, callback)
        b = false
      }catch {case e:Throwable=>
        LOG.error("MySql executeQuery fail, Will try again !!", e)
        Thread.sleep(5000)
      }
    }
    res.asInstanceOf[T]
  }

  private def executeQuery0[T] (sql: String, callback:Callback[T]): T = {
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
        throw new MySQLJDBCClientException("Fail SQL: " + sql + "\nMessage: "+ e.getMessage, e)
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
        throw new MySQLJDBCClientException("Fail SQL: " + sql + "\nMessage: " + e.getMessage, e)
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

object MySqlJDBCClientV2{
  trait Callback[T]{
    def onCallback(rs: ResultSet):T
  }

  def d(): String ={
    println("we")
    "ww"
  }

  def e(x: =>Unit): Unit ={

  }
  def e2( x: =>Any): Unit ={
    
  }

  //TEST
  def main (args: Array[String]): Unit = {
    e2(d)
  }
}


