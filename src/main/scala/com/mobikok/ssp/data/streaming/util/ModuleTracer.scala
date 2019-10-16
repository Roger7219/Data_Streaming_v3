package com.mobikok.ssp.data.streaming.util

import java.text.DecimalFormat
import java.util
import java.util.{Date, Random}

import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.DynamicConfig
import com.mobikok.ssp.data.streaming.exception.AppException
import com.mobikok.ssp.data.streaming.module.Module
import com.mobikok.ssp.data.streaming.module.support.MixModulesBatchController
import com.typesafe.config.Config

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/10/10.
  * recordHistoryBatches: 记录历史批次次数
  */
class ModuleTracer(moduleName: String, config: Config, mixModulesBatchController: MixModulesBatchController, recordHistoryBatches:Integer = 20) {

  def trace(traceMessage: String, waitFor: => Unit): Unit = {
//    pauseBatch()
    updatableTrace(s"$traceMessage [doing]")
    waitFor
//    continueBatch()
    finalUpdatableTrace(traceMessage)
  }

  val LOG: Logger = new Logger(classOf[ModuleTracer])
  //兼容此前的
  def this(){
    this(null, null, null)
  }

  @volatile private var LOCK: Object = new Object
  @volatile private var moduleBatchEndTime: Long = System.currentTimeMillis()
  @volatile private var checkCurrBatchWaitTimePollingThread: Thread = null


  //mixModulesBatchController == null 为了兼容此前的

  if(mixModulesBatchController == null || mixModulesBatchController.isRunnable(moduleName)) {
    initBatchProcessingTimeoutChecker();
  }else {
    LOG.warn("No inited BatchProcessingTimeoutChecker", "moduleName", moduleName)
  }

  private val traceBatchUsingTimeFormat = new DecimalFormat("#####0.00")

  private var batchBeginTime = new ThreadLocal[Long]()
  private var batchContinueTime = new ThreadLocal[Long]()// batchBeginTime //暂停后再次启动的时间

  private var batchUsingTime = new ThreadLocal[Long]() //0.asInstanceOf[Long]
  private var batchActualTime = new ThreadLocal[Long]() //0.asInstanceOf[Long]

  private var lastTraceTime = new ThreadLocal[Long]()//new Date().getTime
  private var traceBatchUsingTimeLog = new ThreadLocal[mutable.ListBuffer[String]]()//mutable.ListBuffer[String]()
  private var threadPrefix = new ThreadLocal[String](){
    override protected def initialValue: String = {
      ""
    }
  }

  private var historyBatchCollector = new FixedList[mutable.ListBuffer[String]](recordHistoryBatches)

  def startBatch: Unit = {
    batchBeginTime.set(new Date().getTime)
    batchContinueTime.set(batchBeginTime.get())
    batchActualTime.set(0)
    batchUsingTime.set(0)
    lastTraceTime.set(batchBeginTime.get())

    var b = mutable.ListBuffer[String]()
    historyBatchCollector.add(b)
    traceBatchUsingTimeLog.set(b)

    trace(s"thread: ${Thread.currentThread().getId}")
  }
  def startBatch(transactionOrder: Long, parentTid: String): Unit = {
    startBatch(transactionOrder, parentTid, null, "")
  }

  def startBatch(transactionOrder: Long, parentTid: String, parentThreadId: java.lang.Long, prefix: String): Unit = {
    batchBeginTime.set(new Date().getTime)
    batchContinueTime.set(batchBeginTime.get())
    batchActualTime.set(0)
    batchUsingTime.set(0)
    lastTraceTime.set(batchBeginTime.get())

    var b = mutable.ListBuffer[String]()
    historyBatchCollector.add(b)
    traceBatchUsingTimeLog.set(b)
    threadPrefix.set(prefix)

    if(parentThreadId != null) {
      trace(s"thread: ${Thread.currentThread().getId}, pThread: ${parentThreadId}, order: ${transactionOrder}, pTid: ${parentTid}")
    }else {
      trace(s"thread: ${Thread.currentThread().getId}, order: ${transactionOrder}, pTid: ${parentTid}")
    }
  }

  def endBatch(): Unit ={
    batchUsingTime.set(new Date().getTime - batchBeginTime.get())
    batchActualTime.set((new Date().getTime - batchContinueTime.get()) + batchActualTime.get())
    moduleBatchEndTime = System.currentTimeMillis()
  }

  def getBatchUsingTime(): Double ={
    (100.0*batchUsingTime.get()/1000/60).asInstanceOf[Int]/100.0
  }

  def getBatchActualTime(): Double ={
    (100.0*batchActualTime.get()/1000/60).asInstanceOf[Int]/100.0
  }

  @deprecated
  def pauseBatch() {
    batchActualTime.set((new Date().getTime - batchContinueTime.get()) + batchActualTime.get())
  }

  @deprecated
  def continueBatch() {
    batchContinueTime.set(new Date().getTime())
    lastTraceTime.set(batchContinueTime.get())
  }

  def trace (title: String) = {
    val ms = System.currentTimeMillis() - lastTraceTime.get()
    val m = traceBatchUsingTimeFormat.format((100.0*ms/1000/60).asInstanceOf[Int]/100.0)
    lastTraceTime.set(new Date().getTime)
    if (traceBatchUsingTimeLog.get() == null) {
      startBatch
    }
    traceBatchUsingTimeLog.get().append(s"${CSTTime.now.time}  $m  ${threadPrefix.get()}$title")
  }

  private def updatableTrace (title: String) = {
    traceBatchUsingTimeLog.get().append(s"${CSTTime.now.time}  ----  $title")
  }

  private def finalUpdatableTrace (title: String) = {
    val ms = new Date().getTime - lastTraceTime.get()
    val m = traceBatchUsingTimeFormat.format((100.0*ms/1000/60).asInstanceOf[Int]/100.0)
    val log = traceBatchUsingTimeLog.get()
    val lastIdx = log.length - 1
    if(lastIdx >= 0) {
      log.remove(lastIdx)
    }
    traceBatchUsingTimeLog.get().append(s"${CSTTime.now.time}  $m  $title")
    lastTraceTime.set(new Date().getTime)
  }

  def getTraceResult (): String ={
    traceBatchUsingTimeLog.get().mkString("\n")
  }

  def getHistoryBatchesTraceResult():String ={
    historyBatchCollector.get().map(_.mkString("\n")).mkString("\n\n")
  }


  private def initBatchProcessingTimeoutChecker(): Unit ={
    LOCK.synchronized{
      if(checkCurrBatchWaitTimePollingThread == null) {
        checkCurrBatchWaitTimePollingThread = new Thread(new Runnable {
          override def run (): Unit = {
            while (true) {
              try{
                //兼容此前的
                if(config != null)  {
                  val b = config.getInt("spark.conf.streaming.batch.buration")
                  val n = config.getString("spark.conf.set.spark.app.name")

//                  val rand = new Random
//                  var maxWaitingTimeMS = 1000*rand.nextInt(1000)
//                  var maxWaitingTimeMS = Math.max(1000*b, 1000*60*60*2)
                  var maxWaitingTimeMS = Math.max(1000*b, 1000*60*45)
//                  var maxWaitingTimeMS = Math.max(1000*b, 1000*60*60*24)
                  val c = DynamicConfig.of(n, DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS)
                  MC.pull(s"${moduleName}_batch_processing_timeout_checker_cer", Array(c), { x=>
                    if(x.nonEmpty) {
                      try {
                        maxWaitingTimeMS = Integer.parseInt(x.last.getKeyBody)
                      }catch {case e:Throwable=>
                       LOG.warn(s"Pull to the invalid  dynamic config '$c' setting value", x.last)
                      }
                    }
                    true
                  })

                  val remainingWaitingTimeMS = maxWaitingTimeMS - (System.currentTimeMillis() - moduleBatchEndTime)

                  LOG.trace("Checking batch processing using time",
                    "remaining_waiting_time_MS", remainingWaitingTimeMS,
                    "max_waiting_time_MS", maxWaitingTimeMS,
                    "buration_seconds", b,
                    "app_name", n,
                    "app_batch_end_time", CSTTime.time(moduleBatchEndTime)
                  )

                  if(remainingWaitingTimeMS < 0){
                    LOG.warn(s"App module batch processing timeout !!!", "important_notice", "Kill self yarn app at once !!!", "app_name", n, "module_name", moduleName, "max_batch_processing_time", (maxWaitingTimeMS/1000.0/60.0) +" minutes")
                    YarnAPPManagerUtil.killApps(n)
                  }
                  Thread.sleep(maxWaitingTimeMS)
                }else {
                  Thread.sleep(Long.MaxValue)
                }

              }catch {case e:Throwable=>
                LOG.error("Polling check current batch wait time occurrence error:", e)
              }
            }
          }
        })

        checkCurrBatchWaitTimePollingThread.start()
      }
    }

  }
}

object ModuleTracer{

//  @volatile private var LOCK: Object = new Object
//  @volatile private var appBatchEndTime: Long = System.currentTimeMillis()
//  @volatile private var checkCurrBatchWaitTimePollingThread: Thread = null

}