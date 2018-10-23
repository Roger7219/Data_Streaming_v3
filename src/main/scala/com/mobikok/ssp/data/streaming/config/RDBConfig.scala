package com.mobikok.ssp.data.streaming.config

import java.sql.ResultSet
import java.util.Date

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util.{MySqlJDBCClient, MySqlJDBCClientV2, StringUtil}

/**
  * Created by Administrator on 2017/8/24.
  */
class RDBConfig(mySqlJDBCClient: MySqlJDBCClientV2) {


  def readRDBConfig (name: String): String = {
//    val rs = mySqlJDBCClient.executeQuery(s"""select value from config where name = "$name" """)
//    if(rs.next()) rs.getString(1) else null

    mySqlJDBCClient.executeQuery(s"""select value from config where name = "$name" """, new Callback[String] {
      override def onCallback (rs: ResultSet): String = {
        if(rs.next()) rs.getString(1) else null
      }
    })
  }

  def readRDBConfig (names: String*): String = {
    var res = null.asInstanceOf[String]
//    val rs = mySqlJDBCClient.executeQuery(s"""select value from config where name = "$name" """)
//    if(rs.next()) rs.getString(1) else null
    var b = true
    var i = 0
    while(b && i < names.length){
      res = mySqlJDBCClient.executeQuery(s"""select value from config where name = "${names(i)}" """, new Callback[String] {
        override def onCallback (rs: ResultSet): String = {
          if(rs.next()) rs.getString(1) else null
        }
      })

      i += 1
      if(StringUtil.notEmpty(res)) {
        b = false
      }
    }

    res
  }

}

object RDBConfig {

  var RDB_CONFIG: RDBConfig = null
  def init(mySqlJDBCClient: MySqlJDBCClientV2): Unit = {
    RDB_CONFIG = new RDBConfig(mySqlJDBCClient)
  }

  def readConfig (name: String): String = {
    if(RDB_CONFIG == null) throw new RuntimeException("RDBConfig is not initialized yet, Please call RDBConfig.init(mySqlJDBCClient: MySqlJDBCClientV2) first.")

    RDB_CONFIG.readRDBConfig(name)
  }

  def readRDBConfig (names: String*): String = {
    if(RDB_CONFIG == null) throw new RuntimeException("RDBConfig is not initialized yet, Please call RDBConfig.init(mySqlJDBCClient: MySqlJDBCClientV2) first.")

    RDB_CONFIG.readRDBConfig(names:_*)
  }


  val SPARK_DATA_STREAMING_STATUS = "spark.data.streaming.status"
  val KYLIN_ENABLE_CREATE_CUBE = "kylin.enable.create.cube"
  val GREENPLUM_WRITE_MODE = "greenplum.write.mode"
  val LOG_SWICTH_STATUS = "log.swicth.status"
}