package com.mobikok.ssp.data.streaming.util

import java.util
import java.util.Collections

import com.mobikok.ssp.data.streaming.OptimizedMixApp.getClass
import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatusV2.Callback

case class KV (fieldName: String, fieldValue: Any)
/**
  * Created by Administrator on 2018/1/23.
  */
object SQLMixUpdater {

  private[this] val LOG = new Logger(getClass)

  private val LOCK = new Object
  private var mySqlJDBCClientV2:MySqlJDBCClientV2 = null
  private var batchBuration: java.lang.Long = null //默认5分钟更新一次

  // (table, (where, (update field, update value) ))
  private var data: util.Map[String, util.HashMap[String, util.HashMap[String, Any]]] = new util.HashMap[String, util.HashMap[String, util.HashMap[String, Any]]]()
    //Collections.synchronizedMap(new util.HashMap[String, util.HashMap[String, util.HashMap[String, Any]]]())

  def addUpdate (table:String, whereFieldName: String, whereFieldValue: Any, updateFieldName: String, updateFieldValue:Any): Unit ={
    addUpdate(table, whereFieldName, whereFieldValue, Array(KV(updateFieldName, updateFieldValue)) )
  }


  def addUpdate (table:String, whereExpr: String, updateFieldName: String, updateFieldValue:Any): Unit ={
    val wExpr = whereExpr//.trim.replaceAll("[ ]*=[ ]*", "=").replaceAll("[ ]*and[ ]*", " and ").replaceAll("[ ]*or[ ]*", " or ")
    addUpdate(table, wExpr, Array(KV(updateFieldName, updateFieldValue)))
  }


  def addUpdate (table:String, whereFieldName: String, whereFieldValue: Any, updateFields: Array[KV] ): Unit ={
    var wExpr: String = null //whereFieldName + "=" + whereFieldValue
    if(whereFieldValue.isInstanceOf[String]) {
      wExpr = s"""${whereFieldName.toUpperCase}="$whereFieldValue""""
    }else {
      wExpr =  s"""${whereFieldName.toUpperCase}=$whereFieldValue"""
    }

    addUpdate(table, wExpr, updateFields)
  }

  def addUpdate (table:String, whereExpr: String, updateFields: Array[KV]): Unit ={
    synchronizedCall{
      var _table = table.toUpperCase
      var tMap = data.get(_table)
      if (tMap == null) {
        tMap = new util.HashMap[String, util.HashMap[String, Any]]()
        data.put(_table, tMap)
      }

      var us = tMap.get(whereExpr)
      if (us == null) {
        us = new util.HashMap[String, Any]()
        tMap.put(whereExpr, us)
      }

      for (f <- updateFields) {
        us.put(f.fieldName, f.fieldValue)
      }
    }
  }

  import scala.collection.JavaConversions._

  def synchronizedCall(callback: => Any): Any ={
    LOCK.synchronized{
      callback
    }
  }

  def execute(): Unit ={
    synchronizedCall{
      val sqls = data.entrySet().foreach{x=>
        val table = x.getKey
        val sqls =x.getValue.entrySet().map{y=>
          val w = y.getKey
          val us = y.getValue.entrySet().map{z=>
            val fn = s"`${z.getKey}`"
            val fv = if(z.getValue == null) {
              "null"
            }else if(z.getValue.isInstanceOf[String]) {
              s""""${z.getValue}""""
            }else {
              s"""${z.getValue}"""
            }
            s"""$fn=$fv"""
          }.mkString("\n  ", ",\n  ", "")
          s"""
            | update $table
            | set $us
            | where $w
          """.stripMargin
        }
        .toArray

        LOG.warn("Time executing sql", "take(10)", sqls.take(10), "count", sqls.length)
        mySqlJDBCClientV2.executeBatch(sqls, 500)
      }

      data = new util.HashMap[String, util.HashMap[String, util.HashMap[String, Any]]]()
    }
  }

  //定时提交
  @volatile private var autoCommitThread: Thread = null;
  private def autoTimeCommitUpdate(){
    if(autoCommitThread == null) {
      autoCommitThread = new Thread(new Runnable {
        override def run (): Unit = {
          try{
            while (true) {
              Thread.sleep(batchBuration)
              execute()
            }
          }catch {case e:Throwable=>
            LOG.error("Auto time commit update sql thread run fail, Will try again!", e)
          }
        }
      })
      autoCommitThread.start()
    }
  }




  def count (): String ={
   s"""countLines: $countLines, countFields: $countFields"""
  }

  def countFields (): Long ={
    var c = 0
    synchronizedCall{
      data.foreach{x=>
        x._2.foreach{y=>
          val s =y._2.entrySet().size()
          c += s
        }
      }
    }
    c
  }

  def countLines (): Long ={
    var c = 0
    synchronizedCall{
      data.foreach{x=>
        x._2.foreach{y=>
          c += 1
        }
      }
    }
    c
  }

  def init(mySqlJDBCClientV2: MySqlJDBCClientV2, batchBuration:Int): Unit ={
    synchronizedCall{
      if(this.mySqlJDBCClientV2 != null) {
        if(!this.mySqlJDBCClientV2.url.equals(mySqlJDBCClientV2.url)){
          throw new RuntimeException("SQLMixUpdater is initialized many times, but the db url must be consistent every time. Original url: " + this.mySqlJDBCClientV2.url + ", New url: " + mySqlJDBCClientV2.url)
        }
      }
      this.mySqlJDBCClientV2 = mySqlJDBCClientV2
      this.batchBuration = batchBuration
      autoTimeCommitUpdate()
    }

  }
  def init(mySqlJDBCClientV2: MySqlJDBCClientV2): Unit ={
    init(mySqlJDBCClientV2, 1000*60*5)
  }
}




object SQLMixUpdaterTest{
  def main (args: Array[String]): Unit = {

//    SQLMixUpdater.addUpdate("OFFER", "ID", 1, "name", "we")
//    SQLMixUpdater.addUpdate("OFFER", "ID", 1, "nam2e", 12312)
//    SQLMixUpdater.addUpdate("OFFER", "ID", 2, "wvv", 1.2312)
//    SQLMixUpdater.addUpdate("OFFER", "ID", 2, "asd", .12312)
//    SQLMixUpdater.addUpdate("OFFER2", "ID", 444, Array(KV("nam2e", 12312), KV("nma3e", "ddd") ))
//    SQLMixUpdater.addUpdate("OFFER2", "ID", 444, Array(KV("namD2e", 12312), KV("nma3e", "ddd") ))

    SQLMixUpdater.addUpdate("OFFER", "ID", 121, "TodayModeCaps", "ASD")
    SQLMixUpdater.addUpdate("OFFER", "ID", 121, Array(
      KV("TodayClickCount", 1),
      KV("TodayShowCount", 23),
      KV("TodayFee", 12.12),
      KV("TodayCaps", 11)
    ))
    SQLMixUpdater.addUpdate("OFFER", "ID", 121, "Cr", 1)



    println("222222")
    SQLMixUpdater.execute()

  }
}
//
//class WhereBuilder{
//
//  private var map: java.util.HashMap[String, Any] = null
//
//  def and(fieldName: String, fieldValue: Any): ValueBuilder ={
//
//  }
//
//  def and(ands: Array[(String, Any)]): ValueBuilder ={
//    throw new RuntimeException("Unspport!!")
//  }
//
//}
//
//
//class ValueBuilder{
//
//  private var map: java.util.HashMap[String, Any] = null
//
//  def value(fieldName: String, fieldValue: Any): ValueBuilder ={
//
//  }
//
//  def and(fieldName: String, fieldValue: Any): ValueBuilder ={
//
//  }
//
//  def and(ands: Array[(String, Any)]): ValueBuilder ={
//    throw new RuntimeException("Unspport!!")
//  }
//
//}
//
//class Where{
//  private val map = new util.HashMap[String, Object]()
//
//  def put(fieldName: String, fieldValue: Object): Where ={
//    map.put(fieldName, fieldValue)
//  }
//}
//
//class Values{
//
//  private val map = new util.HashMap[String, Object]()
//
//  def put(fieldName: String, fieldValue: Object): Values ={
//    map.put(fieldName, fieldValue)
//  }
//}
//
//
//
