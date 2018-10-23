package com.mobikok.ssp.data.streaming

import java.sql.ResultSet
import java.util.Date

import com.mobikok.ssp.data.streaming.util.{MySqlJDBCClientV2, OM, ZipUtil}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback

/**
  * Created by Administrator on 2018/1/17.
  */
object AdxThirdPartyReportTest {

  def adx_third_party_report(): Unit ={
    var mySqlJDBCClientV2 = new MySqlJDBCClientV2(
      "",
      "jdbc:mysql://node17:3306/sight",
      "root",
      "root_root")

    mySqlJDBCClientV2.executeQuery("select data from sight.message where topic = \"adx_third_party_report\"", new Callback[String] {
      override def onCallback (rs: ResultSet): String = {
        rs.next()
        println("t1"+new Date())

        val s2 = rs.getString("data")
        println("s2.length" + s2.length)

        val json2 = new String( ZipUtil.decompressFromBase64(rs.getString("data")) )
        println("json2.length" + json2.length)

        println("t2"+new Date())

        println( OM.toBean(json2, classOf[java.util.List[Object]]).size );
        println("t3" + new Date())

        //          println(ZipUtil2.decompressFromBase64(rs.getString("data")))
        ""
      }
    })

  }

  def main (args: Array[String]): Unit = {
    adx_third_party_report
  }
}
