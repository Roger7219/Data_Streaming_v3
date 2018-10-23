package com.mobikok.ssp.data.streaming.util

import java.util.concurrent.{CountDownLatch, TimeUnit}

object ThreadPool {

  def concurrentExecute(runnables: Runnable*): Unit = {
    val threadPool = ExecutorServiceUtil.createdExecutorService(runnables.length)
    val countDownLatch = new CountDownLatch(runnables.length)
    for (r <- runnables) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            r.run()
          } catch {
            case e: Exception => throw new RuntimeException("concurrent task error")
          } finally {
            countDownLatch.countDown()
          }
        }
      })
    }
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
    threadPool.shutdown()
  }


  // 在多层嵌套的线程池调用中如果线程池的数量太少会造成死锁,最少数量为最底层之前所有线程之和+1
  private val threadPool = ExecutorServiceUtil.createdExecutorService(100)

  def concurrentExecuteStatic(runnables: Runnable*): Unit = {
    val countDownLatch = new CountDownLatch(runnables.length)
    for (r <- runnables) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            r.run()
          } catch {
            case e: Exception => throw new RuntimeException("concurrent task error")
          } finally {
            countDownLatch.countDown()
          }
        }
      })
    }
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }

  def concurrentExecute_v2(runnables: Runnable*): Unit = {
    val countDownLatch = new CountDownLatch(runnables.length)
    for (r <- runnables) {
      new Thread(new Runnable {
        override def run(): Unit = {
          try {
            r.run()
          } catch {
            case e: Exception => throw new RuntimeException("concurrent task error")
          } finally {
            countDownLatch.countDown()
          }
        }
      }).start()
    }
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }

  def concurrentExecuteStatic(r1: => Unit): Unit = {
    val countDownLatch = new CountDownLatch(1)
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r1
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }

  def concurrentExecuteStatic(r1: => Unit, r2: => Unit): Unit = {
    val countDownLatch = new CountDownLatch(2)
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r1
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r2
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }

  def concurrentExecuteStatic(r1: => Unit, r2: => Unit, r3: => Unit): Unit = {
    val countDownLatch = new CountDownLatch(3)
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r1
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r2
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r3
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }

  def concurrentExecuteStatic(r1: => Unit, r2: => Unit, r3: => Unit, r4: => Unit): Unit = {
    val countDownLatch = new CountDownLatch(4)
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r1
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r2
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r3
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    threadPool.execute(new Runnable {
      override def run(): Unit = {
        try {
          r4
        } catch {
          case e: Exception => throw new RuntimeException("concurrent task error", e)
        } finally {
          countDownLatch.countDown()
        }
      }
    })
    // 无限等待，超过45分钟后通过killself自毁然后重启rollback
    countDownLatch.await()
  }
}
