package com.mobikok.ssp.data.streaming.concurrent

import java.util

import com.mobikok.ssp.data.streaming.App.getClass
import com.mobikok.ssp.data.streaming.util.OM
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/8/23.<br>
  * 用GlobalAppRunningStatusV2代替
  */
@deprecated
object GlobalAppRunningStatus {

  private[this] val LOG = Logger.getLogger(GlobalAppRunningStatus.getClass.getName)

  import scala.collection.JavaConverters._

  import scala.collection.JavaConversions._

  def allStatusMap = statusMap.map{x=>
    x._1 -> x._2.asJava
  }.asJava

  private val lock = new Object

  val RUNNING = "running"
  val IDLE = "idle"

  @volatile private var statusMap = mutable.Map[String, mutable.Map[String,String]]()
  @volatile private var queue = new util.LinkedList[Array[String]]()
  @volatile private var globalLock = false

  def acquiresRunAndSetRunningStatus (concurrentGroup: String, acquiresModule: String): Boolean ={

    LOG.warn(
      s"Acquires run DEMAND  [${Thread.currentThread().getId}] [ $concurrentGroup, $acquiresModule ]"
    )
    queue.offer(Array(Thread.currentThread().hashCode() +"_"+ Thread.currentThread().getId))

    lock.synchronized{
      var b = true
      while (b) {

        if(isAllIdleStatus(concurrentGroup) && (Thread.currentThread().hashCode() + "_"+ Thread.currentThread().getId).equals(queue.peek()(0))) {
          b = false
          queue.poll()
        }else {
            lock.wait(1000*2)
        }
      }

      if(isAllIdleStatus(concurrentGroup)){
        LOG.warn(s"Concurrent group  Status [ ${Thread.currentThread().getId}] [group: $concurrentGroup] \n"+OM.toJOSN(allStatusMap.get(concurrentGroup)))
        setStatus(concurrentGroup, acquiresModule, RUNNING)
        LOG.warn(s"Acquires run STARTING [${Thread.currentThread().getId}] [ $concurrentGroup, $acquiresModule, RUNNING ]"
        )
        return true
      }
      return false
    }

  }
  //所有的都是空闲状态
  private def isAllIdleStatus (concurrentGroup: String): Boolean = {

      while(globalLock) {
        Thread.sleep(1000)
      }
      globalLock = true

      val map = statusMap.get(concurrentGroup).getOrElse(mutable.Map[String,String]())
      var hasNotIdle  = false

      map.foreach{x=>
        if(!IDLE.equals(x._2)) {
          hasNotIdle = true
        }
      }
      globalLock = false

      !hasNotIdle
  }


  def setStatus (concurrentGroup: String, module:String, statusValue: String): Unit = {
    while(globalLock) {
      Thread.sleep(1000)
    }
    globalLock = true

    lock.synchronized{
      var map:mutable.Map[String, String] =  null
      if(statusMap.get(concurrentGroup).isEmpty){
        map = mutable.Map[String, String]()
      }else {
        map = statusMap.get(concurrentGroup).get
      }
      map.+=( (module, statusValue) )

      statusMap.+=( (concurrentGroup, map) )
      LOG.warn(s"Concurrent group setStatus, concurrentGroup: $concurrentGroup, module: $module, statusValue: $statusValue")
    }
    globalLock = false

  }

}



//
//object X{
//  def main (args: Array[String]): Unit = {
//    new Thread(new Runnable {
//      override def run (): Unit = {
//
//        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName1")
//        println("set IDLE")
//        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName1", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
////    Thread.sleep(1000)
////    println("xx22")
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName2")
//
//        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName2", GlobalAppRunningStatus.IDLE)
//        println("set IDLEed")
//      }
//    }).start()
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName3")
//        println("set IDLE ed")
//        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName3", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
////
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName4")
//        println("set IDLE ed")
//        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName4", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
//////    new Thread(new Runnable {
//////      override def run (): Unit = {
//////        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName5")
//////        println("set IDLE")
//////        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName5", GlobalAppRunningStatus.IDLE)
//////      }
//////    }).start()
//////    new Thread(new Runnable {
//////      override def run (): Unit = {
//////        GlobalAppRunningStatus.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName5")
//////        println("set IDLE")
//////        GlobalAppRunningStatus.setStatus("concurrentGroup1", "moduleName5", GlobalAppRunningStatus.IDLE)
//////      }
//////    }).start()
//  }
//}