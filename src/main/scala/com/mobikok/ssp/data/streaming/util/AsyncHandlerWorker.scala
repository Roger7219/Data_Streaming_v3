package com.mobikok.ssp.data.streaming.util

import java.util
import java.util.Date
import java.util.concurrent.{CountDownLatch, ExecutorService}

object AsyncHandlerWorker{
  var defaultPoolSize = 100
  var THREAD_POOL: ExecutorService = ExecutorServiceUtil.createdExecutorService(defaultPoolSize)
}

class AsyncHandlerWorker(moduleName: String, taskSize:Int, moduleTracer: ModuleTracer, transactionOrder: Long, parentTid: String, parentThreadId: Long){
  var LOG:Logger = new Logger(moduleName, getClass().getName(), new Date().getTime())
  var countDownLatch: CountDownLatch = new CountDownLatch(taskSize)
  var syncTaskErrors: util.List[Throwable] = new util.ArrayList[Throwable]()

  startAsyncHandlersCountDownHeartbeats()

  def run(func: => Unit){

    moduleTracer.startAsyncHandle(transactionOrder, parentTid,  parentThreadId)

    AsyncHandlerWorker.THREAD_POOL.execute(new Runnable() {
      def run() {
        try {
          func
          countDownLatch.countDown()
        }catch {
          case t:Throwable=> syncTaskErrors.add(t)
        }
      }
    })
  }


  def await(){
    LOG.warn("Wait AsyncWorker async action [start]")
    var wait = true
      while (wait) {
        try {
          if(syncTaskErrors.size() > 0) {
            throw syncTaskErrors.get(0)
          }

          if(countDownLatch.getCount() == 0)  wait = false

          syncTaskErrors.clear()

        } catch {
          case e:Throwable=> throw new RuntimeException("Async execute error: ", e)
        }

        try{
          Thread.sleep(100)
        }catch {case _:Exception=>}
      }
    LOG.warn("Wait AsyncWorker all async action [done]")
  }

  def startAsyncHandlersCountDownHeartbeats() {
    new Thread(new Runnable() {
      def run() {
        var b = true
        while (b) {
          try {
            if (countDownLatch.getCount() > 0) {
                LOG.warn("Async tasks heartbeats","module name", moduleName, "running tasks", countDownLatch.getCount(), "completed tasks", taskSize - countDownLatch.getCount())
            } else {
                b = false
            }

            Thread.sleep(1000 * 60);
          } catch {
            case e:Exception=> LOG.error("Async task heartbeats error", e);
          }
        }
        }
      }).start()
  }

}
























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
