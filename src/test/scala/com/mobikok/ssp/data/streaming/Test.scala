package com.mobikok.ssp.data.streaming

import java.net.URLDecoder
import java.sql.ResultSet
import java.util.Date

import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient.Callback
import com.mobikok.ssp.data.streaming.util.{MySqlJDBCClient, OM, ZipUtil}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/6/6.
  */
object Test {

//  PRO 外网IP: 104.250.149.202
//  rdb.url = "jdbc:mysql://192.168.111.22:4000/kok_ssp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"
//  rdb.user = "root"
//  rdb.password = "@dfei$@DCcsYG"
//
  def adx_third_party_report(): Unit ={
    var mySqlJDBCClientV2 = new MySqlJDBCClient(
      "test",
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

  def traceBatchUsingTime (title: String, lastTraceTime: Array[Long], traceBatchUsingTimeLog: ListBuffer[String]) = {
    traceBatchUsingTimeLog.append(s"$title: ${(100.0* (new Date().getTime-lastTraceTime(0)) /1000/60).asInstanceOf[Int]/100.0}")
    lastTraceTime.update(0, new Date().getTime)
  }


  def main (args: Array[String]): Unit = {
    adx_third_party_report()

    //    val s22= Array(new Date().getTime)
//
//    val s2222= ListBuffer("")
//
//    traceBatchUsingTime("t", s22, s2222)
//
//    Thread.sleep(1000*10)
//
//    traceBatchUsingTime("t", s22, s2222)
//    println(s2222.mkString("\n"))
//
//
//    println()
//    println()
//    println()
//


    //
//    var sss= ListBuffer[String]()
//    sss.append( "asd")
//    sss.append( "a2sd")
//    print(sss.mkString("\n"))

 println(   (100.0*1090*60/1000/60).asInstanceOf[Int]/100.0)

    val s2=null.asInstanceOf[String]
    println(s"""$s2 """)
    println(URLDecoder.decode("ASD%3AQWE", "utf-8"))


    println( java.util.Arrays.deepToString(Array("asd", "we")))
    val s=Array("asd","23")++Array("rt")

//    println(s(0)  +" ! " +s(1) + " ! "+s(2))
//    println(s.head +" ! " + java.util.Arrays.deepToString( s.tail.asInstanceOf[Array[Object]]))

    ss("as","dd")
    ss("as","dd","333")
    ss("as","dd","333",11)



    //
//    println(classOf[UuidStat])
//    println(classOf[Object])

    // val s =List[String]("asd")
    // val oneTwoThree = "asd" ::  "asd" :: Nil // List(1, 2, 3)
  }

  def ss(xx : Any, xx2: Any*): Unit ={

    println(xx2.size)
  }
//  def main(args: Array[String]): Unit = {
//    var dataFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd-000000")
//
//    val l_date = dataFormat.format(new Date())
//    println(l_date)
//    print( s""""l_date": $l_date,""")
//  }
}
