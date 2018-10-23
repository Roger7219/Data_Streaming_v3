package com.mobikok.ssp.data.streaming.concurrent

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SaveMode

import scala.collection.mutable

/**
  * Created by Administrator on 2017/8/23.
  */
@deprecated
class ConcurrentGroupManager {


}


//
//object X2{
//  def main (args: Array[String]): Unit = {
//    compaction("table")
//    compaction("table")
//    compaction("table2")
//    compaction("table2")
//  }
//  val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
//  //table -> l_date -> needCompaction
//  var yesterdayPartitionNeedCompactionMap = mutable.Map[String, mutable.Map[String, Boolean]]()
//  private def compaction(table : String): Unit = {
//
//    val yest = dataFormat.format(new Date(new Date().getTime - 24*60*60*1000))
//
//    val lnOpt = yesterdayPartitionNeedCompactionMap.get(table)
//    var ln: mutable.Map[String, Boolean] = null
//    var need = false
//    if(lnOpt.isEmpty) {
//      need = true
//      ln = mutable.Map[String, Boolean]()
//      yesterdayPartitionNeedCompactionMap.put(table, ln)
//    }else {
//      need = lnOpt.get.getOrElse(yest, true)
//      ln = lnOpt.get
//    }
//
//    //
//    if(need) {
//
//      println(s"$table sssssssssss")
//
//    }
//    //移除非昨天的
//    val keys= ln.keys
//    keys.foreach{x=>
//      if(!yest.equals(x)) {
//        ln.remove(x)
//      }
//    }
//
//    ln.put(yest, false)
//  }
//}