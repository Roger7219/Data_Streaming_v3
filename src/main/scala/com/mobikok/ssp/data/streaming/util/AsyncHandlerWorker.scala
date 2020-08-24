package com.mobikok.ssp.data.streaming.util

import java.util
import java.util.Date
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CountDownLatch, ExecutorService}

import com.mobikok.ssp.data.streaming.exception.AsyncHandlerWorkerException

object AsyncHandlerWorker{
  var defaultPoolSize = 100
  var THREAD_POOL: ExecutorService = ExecutorServiceUtil.createdExecutorService(defaultPoolSize)
}

class AsyncHandlerWorker(moduleName: String, totalTaskSize:Int, moduleTracer: ModuleTracer, transactionOrder: Long, parentTransactionId: String, parentThreadId: Long){
  var LOG = new Logger(moduleName, getClass, new Date().getTime())
  var countDownLatch: CountDownLatch = new CountDownLatch(totalTaskSize)
  var runningTasks: AtomicInteger = new AtomicInteger()
  var syncTaskErrors: util.List[Throwable] = new util.ArrayList[Throwable]()

  startAsyncHandlersCountDownHeartbeats()

  def run(func: => Unit){

    AsyncHandlerWorker.THREAD_POOL.execute(new Runnable() {
      def run() {
        try {
          runningTasks.incrementAndGet()
          moduleTracer.startNested(transactionOrder, parentTransactionId,  parentThreadId)

          func
          countDownLatch.countDown()
        }catch {
          case t:Throwable=> syncTaskErrors.add(t)
        }
      }
    })
  }


  def await(){
    LOG.warn("Wait AsyncHandlerWorker async action [start]")
    var wait = true
      while (wait) {
        try {
          if(syncTaskErrors.size() > 0) {
            throw syncTaskErrors.get(0)
          }

          if(countDownLatch.getCount() == 0)  wait = false

          syncTaskErrors.clear()

        } catch {
          case e:Throwable=> throw new AsyncHandlerWorkerException("AsyncHandlerWorker execute error: ", e)
        }

        try{
          Thread.sleep(100)
        }catch {case _:Exception=>}
      }
    moduleTracer.trace("wait async handlers done")
    LOG.warn("Wait AsyncHandlerWorker all async action [done]")
  }

  def startAsyncHandlersCountDownHeartbeats() {
    new Thread(new Runnable() {
      def run() {
        var b = true
        while (b) {
          try {
            if (countDownLatch.getCount() > 0) {
                LOG.warn("AsyncHandlerWorker tasks heartbeats","module name", moduleName, "not started tasks", totalTaskSize - runningTasks.get(),"running tasks", runningTasks.get(), "completed tasks", totalTaskSize - countDownLatch.getCount())
            } else {
                b = false
            }

            Thread.sleep(1000 * 60);
          } catch {
            case e:Exception=> LOG.error("AsyncHandlerWorker task heartbeats error", e);
          }
        }
        }
      }).start()
  }

}




















//class module(name: String){
//  var threadLocal = new InheritableThreadLocal[String]
//  threadLocal.set(name)
//  def getname(): String ={
////    threadLocal.get()
//    name
//  }
//
//  override def toString: String = {" name: "+name}
//}
//
//
//
//object Test {
//
//  def main(args: Array[String]): Unit = {
//
//    var threadLocalModules = new InheritableThreadLocal[module]
//
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        val m1 =new module("m1")
//        val m1name = "m1"
//        threadLocalModules.set(m1)
//
//        AsyncHandlerWorker.THREAD_POOL.execute(new Runnable {
//          override def run(): Unit = {
////            Thread.sleep(1000*10)
//            println(Thread.currentThread().getId+": m1name:" +  m1name +" : "+ threadLocalModules.get().getname())
//          }
//        })
//        List(111111,222222).par.foreach{x=>
//          new Thread(new Runnable {
//            override def run(): Unit = {
//              println(s"${Thread.currentThread().getId}: m1 par> "+threadLocalModules.get().getname())
//            }
//          }).start()
//        }
//      }
//    }).start()
//
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        val m2 =new module("m2")
//        val m2name = "m2"
//        threadLocalModules.set(m2)
//
//        AsyncHandlerWorker.THREAD_POOL.execute(new Runnable {
//          override def run(): Unit = {
////            Thread.sleep(1000*10)
//            println(Thread.currentThread().getId+": m2name: "+ m2name+" : " + threadLocalModules.get().getname())
//          }
//        })
//
//        var x= List(111111,222222).par
//          x.foreach{x=>
//          new Thread(new Runnable {
//            override def run(): Unit = {
//              println(s"${Thread.currentThread().getId}: m2 par> "+threadLocalModules.get().getname())
//            }
//          }).start()
//        }
//      }
//    }).start()
////
//    println(s"main>"+threadLocalModules.get())
//    List(1,2).par.foreach{x=>
//      new Thread(new Runnable {
//        override def run(): Unit = {
//          println(s"main par>"+threadLocalModules.get())
//        }
//      }).start()
//
//    }
//
////    AsyncHandlerWorker.THREAD_POOL.execute(new Runnable {
////      override def run(): Unit = {
////        System.out.println("THREAD_POOL = " + threadLocal.get)
////        threadLocal.set(333)
////            val thread = new Test.MyThread
////            thread.start()
////      }
////    })
////    System.out.println("main = " + threadLocal.get)
//  }
//
////  class MyThread extends Thread {
////    override def run(): Unit = {
////      System.out.println("MyThread = " + threadLocal.get)
////    }
////  }
//
//}




















// JAVA 版
//package com.mobikok.ssp.data.streaming.util;
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//
//public class AsyncWorker {
//
//    private Logger logger;
//
//    private String name;
//    private CountDownLatch countDownLatch;
//    private volatile List<Throwable> syncErrors;
//    private static ExecutorService threadPool = ExecutorServiceUtil.createdExecutorService(100);
//
//    public AsyncWorker(String name, int taskSize){
//        this.name = name;
//        this.countDownLatch = new CountDownLatch(taskSize);
//        this.syncErrors = ;
//        logger = new Logger(name, this.getClass().getName(), new Date().getTime());
//        startAsyncHandlersCountDownHeartbeats();
//    }
//
//    public void run(Runnable task){
//        threadPool.execute(new Runnable() {
//            public void run() {
//                try {
//                    task.run();
//                    countDownLatch.countDown();
//                }catch (Throwable t) {
//                    syncErrors.add(t);
//                }
//            }
//        });
//    }
//
//    public void await(){
//        while (true) {
//
//            try {
//
//                if(syncErrors.size() > 0) {
//                    throw syncErrors.get(0);
//                }
//
//                if(countDownLatch.getCount() == 0){
//                    return;
//                }
//
//                Thread.sleep(100);
//            } catch (Throwable e) {
//                throw new RuntimeException("Async execute error: ", e);
//            }
//        }
//    }
//
//    public void startAsyncHandlersCountDownHeartbeats() {
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                boolean b = true;
//                while (b) {
//                    try {
//                        if (countDownLatch.getCount() > 0) {
//                            logger.warn(name + " async running task count: " + countDownLatch.getCount());
//                        } else {
//                            b = false;
//                        }
//
//                        Thread.sleep(1000 * 60);
//                    } catch (Exception e) {
//                        logger.error("Async task heartbeats error",e);
//                    }
//                }
//            }
//        }).start();
//    }
//
//}
