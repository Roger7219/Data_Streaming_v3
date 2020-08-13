package com.mobikok.ssp.data.streaming

import java.sql.ResultSet

import com.mobikok.ssp.data.streaming.util.{CSTTime, MySqlJDBCClient, RegexUtil}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient.Callback

/**
  * Created by Administrator on 2017/8/7.
  */
object MySqlDeleteSightTXTmpTableTest {

  def main (args: Array[String]): Unit = {

    val rdbUser = "root"
//    val rdbPassword = "@dfei$@DCcsYG"//
    val rdbPassword = "%oEL!L#Lkf&B!$F9JapY"
    //
    val rdbUrl = "jdbc:mysql://node17:3306/sight?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
//    val rdbUrl = "jdbc:mysql://104.250.131.130:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
//    val rdbUrl = "jdbc:mysql://192.168.1.244:8904/kok_ssp?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf8"
    val rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

    val mySqlJDBCClient = new MySqlJDBCClient( rdbUrl, rdbUser, rdbPassword
    )

//    mySqlJDBCClient.executeQuery("select * from offset_m_advertiser_am_ts_20200201_023006_188__0", new Callback[String](){
//      override def onCallback(rs: ResultSet): String = {
//        rs.next()
//        println(rs.getObject(1))
//        ""
//      }
//    })


    val tabs = Array("_ts_", "_bc_", "_bp_")

    for(t <- tabs) {
      mySqlJDBCClient.executeQuery(s"""show tables like "%${t}%" """, new Callback[Unit] {
        override def onCallback(rs: ResultSet): Unit = {
          while (rs.next()) {
            val t = rs.getString(1)
            // 正则匹配时间
            RegexUtil.matchedGroups(t, "([0-9]{4}[0-1][0-9][0-3][0-9]_[0-2][0-9][0-6][0-9][0-6][0-9])_[0-9]{3}__[0-9]")
              .map{x=>
                // 删除1天以前的事务表
                if( System.currentTimeMillis() - CSTTime.ms(x,"yyyyMMdd_HHmmss") > 1L*24*60*60*1000) {
                  mySqlJDBCClient.execute(s"drop table if exists ${t}")
                }
              }
          }
        }
      })


    }

    println("clean done !!!")



  }
}
