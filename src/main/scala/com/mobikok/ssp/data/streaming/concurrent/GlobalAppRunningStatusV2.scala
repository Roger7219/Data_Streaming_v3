package com.mobikok.ssp.data.streaming.concurrent

import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.concurrent.GlobalAppRunningStatus.globalLock
import com.mobikok.ssp.data.streaming.util.{Logger, OM}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/8/23.
  */
object GlobalAppRunningStatusV2 {

  private[this] val LOG = new Logger(GlobalAppRunningStatusV2.getClass)//Logger.getLogger(GlobalAppRunningStatusV2.getClass.getName)

  import scala.collection.JavaConverters._

  import scala.collection.JavaConversions._

  def allStatusToString = OM.toJOSN(STATUS_MAP.map{ x=>
    x._1 -> x._2.map{y=>y._1 -> s"${y._2._2}: ${y._2._1}" }.asJava
  }.asJava)

  private val LOCK = new Object

  val STATUS_RUNNING = "RUNNING"
  val STATUS_IDLE = "IDLE"

  @volatile private var STATUS_MAP = mutable.Map[String, mutable.Map[String, (String, Long)]]()
  //moduleName,
  @volatile private var MODULE_QUEUE = new util.HashMap[String, util.LinkedList[(Long, String)]]()

//  @volatile private var queue = new util.LinkedList[Array[Long]]()
  @volatile private var GLOBAL_LOCK = new Object


  def isPreviousRunning(concurrentGroup: String, acquiresModule:String): Boolean ={
    val queue = MODULE_QUEUE.get(concurrentGroup)
    queue != null && queue.filter{x=>x._2.equals(acquiresModule)}.nonEmpty
  }

  var hasWinner = mutable.Map[String, Boolean]()
  def waitRunAndSetRunningStatus (concurrentGroup: String, acquiresModule: String) {

    // list<threadId, acquiresModule>
    var queue = null.asInstanceOf[util.LinkedList[(Long, String)]]
    LOCK.synchronized{
      LOG.warn("Joining module running queue", "moduleName", acquiresModule, "threadId", Thread.currentThread().getId, "concurrentGroup", concurrentGroup, "allCurrentStatus", allStatusToString)
      queue = MODULE_QUEUE.get(concurrentGroup)
      if(queue == null) {
        queue = new util.LinkedList[(Long, String)]()
        MODULE_QUEUE.put(concurrentGroup, queue)
      }
      queue.offer((Thread.currentThread().getId, acquiresModule))
//      queue.offer(Array(Thread.currentThread().getId))
    }

    var b = true
    while (b) {
      if(new Date().getSeconds%60 == 0) {
        LOG.info(s"Show waiting module queue", "waitingQueueThreads", queue.mkString("\n"), "concurrentGroup", concurrentGroup, "allCurrentStatus", allStatusToString)
      }

      if(isAllIdleStatus(concurrentGroup) && queue.peek() != null && queue.peek()._1 == Thread.currentThread().getId /*&& !hasWinner.getOrElse(concurrentGroup, false)*/) {
        LOCK.synchronized{
          //LOG.info(s"Checking running status", "Waiting queue threads", queue.mkString("\n"), "concurrentGroup", concurrentGroup)

          /*if(hasWinner.getOrElse(concurrentGroup, false)) {
            Thread.sleep(500)
          }else {*/
            b = false
            val runningModule = queue.poll()
//            hasWinner.put(concurrentGroup, true)

            //
            LOG.warn(s"Module start running", "moduleName", runningModule._2, "concurrentGroup", concurrentGroup, "threadId", Thread.currentThread().getId)
            setStatus(concurrentGroup, acquiresModule, STATUS_RUNNING)
          //            hasWinner.put(concurrentGroup, false)
//          }
        }
      }else {
        LOCK.synchronized{
          LOCK.wait(2000)
        }
//          Thread.sleep(1000)
      }

    }



//    if(isAllIdleStatus(concurrentGroup)){
//      LOG.warn(s"Concurrent group  Status [ ${Thread.currentThread().getId}] [group: $concurrentGroup] \n"+OM.toJOSN(allStatusMap.get(concurrentGroup)))
//      setStatus(concurrentGroup, acquiresModule, RUNNING)
//      LOG.warn(s"Acquires run STARTING [${Thread.currentThread().getId}] [ $concurrentGroup, $acquiresModule, RUNNING ]"
//      )
//      hasWinner.put(concurrentGroup, false)
//      return true
//    }
//    return false

  }
  //所有的都是空闲状态
  private def isAllIdleStatus (concurrentGroup: String): Boolean = {
//
//    while(GLOBAL_LOCK) {
//      Thread.sleep(500)
//    }
//    GLOBAL_LOCK = true
    var hasNotIdle  = false

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
          val map = STATUS_MAP.get(concurrentGroup).getOrElse(mutable.Map[String,(String, Long)]())
          map.foreach{x=>
            if(!STATUS_IDLE.equals(x._2._1)) {
              hasNotIdle = true
            }
          }
      }
    }, GLOBAL_LOCK)

//    GLOBAL_LOCK = false

    !hasNotIdle
  }

  def setStatus (concurrentGroup: String, module:String, statusValue: String): Unit = {
//    while(GLOBAL_LOCK) {
//      Thread.sleep(1000)
//    }
//    GLOBAL_LOCK = true

    synchronizedCall(new Callback {
      override def onCallback (): Unit = {
        var map:mutable.Map[String, (String, Long)] =  null
        if(STATUS_MAP.get(concurrentGroup).isEmpty){
          map = mutable.Map[String, (String, Long)]()
        }else {
          map = STATUS_MAP.get(concurrentGroup).get
        }
        map.+=( (module, (statusValue, Thread.currentThread().getId)) )

        STATUS_MAP.+=( (concurrentGroup, map) )
        LOG.warn(s"Changed running status", "moduleName", module, "concurrentGroup", concurrentGroup, "changeToStatus", statusValue, "changeByThread", Thread.currentThread().getId, "allCurrentStatus", allStatusToString)
      }
    }, GLOBAL_LOCK)

//    GLOBAL_LOCK = false

  }


  def synchronizedCall(callback: Callback, lock: Object): Unit ={
    lock.synchronized{
      callback.onCallback()
    }
  }

  trait Callback{
    def onCallback()
  }
}




//
//object X22{
//  def main (args: Array[String]): Unit = {
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName1")
//        println("set IDLE")
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup1", "moduleName1", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup2", "moduleName1")
//        println("set IDLE")
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup2", "moduleName1", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
////    Thread.sleep(1000)
////    println("xx22")
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName2")
//
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup1", "moduleName2", GlobalAppRunningStatus.IDLE)
//        println("set IDLEed")
//      }
//    }).start()
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName3")
//        println("set IDLE ed")
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup1", "moduleName3", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
////
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup2", "moduleName2")
//        println("set IDLE")
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup2", "moduleName2", GlobalAppRunningStatus.IDLE)
//      }
//    }).start()
//
//    new Thread(new Runnable {
//      override def run (): Unit = {
//        GlobalAppRunningStatusV2.acquiresRunAndSetRunningStatus("concurrentGroup1", "moduleName4")
//        println("set IDLE ed")
//        GlobalAppRunningStatusV2.setStatus("concurrentGroup1", "moduleName4", GlobalAppRunningStatus.IDLE)
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