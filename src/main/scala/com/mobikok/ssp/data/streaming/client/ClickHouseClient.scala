package com.mobikok.ssp.data.streaming.client

import java.io.InputStream
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.Charset
import java.util.Date
import java.util.concurrent.{CountDownLatch, ExecutorService}

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util._
import com.mobikok.ssp.data.streaming.util.http.{Callback, Entity, Requests}
import com.typesafe.config.Config
import okio.Okio
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class ClickHouseClient(moduleName: String, config: Config, ssc: StreamingContext,
                       messageClient: MessageClient, hiveContext: HiveContext) {

  private val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
  private val conf = new HdfsConfiguration()
  private val fileSystem = FileSystem.get(conf)

  //  private val dateFormat = CSTTime.formatter("yyyyMMddHHmmss")
  private val hosts = config.getStringList("clickhouse.hosts")
  private var THREAD_POOL = null.asInstanceOf[ExecutorService]
  val THREAD_COUNT = 2
  val dataCountMap = new FixedLinkedMap[String, Long](THREAD_COUNT)
  //ExecutorServiceUtil.createdExecutorService(5)
  //  private val semaphore = new Semaphore(2)

  //  private val countDownLatch = new CountDownLatch()

  @volatile private var lastLengthMap: FixedLinkedMap[String, java.lang.Long] = new FixedLinkedMap[String, java.lang.Long](24 * 3)

  @deprecated
  def overwriteByBDate(clickHouseTable: String, hiveTable: String, hivePartitionBDates: Array[String]): Unit = {

    LOG.warn(s"Overwrite clickHouseTable start", "clickHouseTable", clickHouseTable, "hiveTable", hiveTable)

    if (hosts == null || hosts.size() == 0) {
      throw new NullPointerException("ClickHouse hosts length is 0")
    }

    if (THREAD_POOL == null || THREAD_POOL.isShutdown) {
      THREAD_POOL = ExecutorServiceUtil.createdExecutorService(THREAD_COUNT)
    }

    hivePartitionBDates.foreach { b_date =>
      //        THREAD_POOL.execute(new Runnable {
      //          override def run(): Unit = {
      //            semaphore.acquire()
      val time = b_date.replace("-", "_")
      val gzDir = s"/root/kairenlo/data-streaming/test/${hiveTable}_${time}_gz.dir"
      var gzFiles = null.asInstanceOf[Array[FileStatus]]
      RunAgainIfError.run {
        val fields = hiveContext
          .read
          .table(hiveTable)
          .schema
          .fieldNames
          .map { name => s"$name as ${name.toLowerCase}" }

        hiveContext
          .read
          .table(hiveTable)
          .selectExpr(fields: _*)
          .where(s""" b_date = "$b_date" """)
          //          .repartition(1)
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("json")
          .option("compression", "gzip")
          .save(gzDir)

        LOG.warn(s"gz file saved completed at $gzDir")

        gzFiles = fileSystem.listStatus(new Path(gzDir), new PathFilter {
          override def accept(path: Path): Boolean = {
            path.getName.startsWith("part-")
          }
        })

        // 其实可以多个文件
        if (gzFiles.length > 1) {
          throw new RuntimeException(s"ClickHouse must upload only one file, but now it finds ${gzFiles.length} in '$gzDir' !!")
        }

        // 验证数据完整性
        gzFiles.foreach { file =>
          val current = file.getLen
          val last = lastLengthMap.get(s"$hiveTable^$b_date")
          if (last != null && current * 1.5 < last) {
            throw new RuntimeException(s"Hive table '$hiveTable' b_date = '$b_date' reading data are incomplete, less than before.")
          }
          lastLengthMap.put(s"$hiveTable^$b_date", current)
        }

        LOG.warn("ClickHouseClient generate gz file completed", gzDir)

        val b_time = s"$b_date 00:00:00"
        dropPartition(clickHouseTable, b_date, b_time)

        if (gzFiles == null || gzFiles.length <= 0) {
          LOG.warn("gzFiles is error!")
          throw new HandlerException(s"gzFiles size = ${gzFiles.length}")
        }

        gzFiles.foreach { file =>
          // 文件在这里出现合并后找不到文件时重新导出文件
          val in = fileSystem.open(file.getPath)
          LOG.warn(file.getPath.toString)
          if (uploadToClickHouse(clickHouseTable, hosts(0), in)) {
            // 需要等待一段时间导入数据
            Thread.sleep(5000)
            LOG.warn(s"copy to ${clickHouseTable}_for_select")
            copyToClickHouseForSearch(clickHouseTable, s"${clickHouseTable}_for_select", b_date, b_time)
            dropPartition(clickHouseTable, b_date, b_time)
          }
        }
        LOG.warn(s"Upload success at partition ($b_date, $b_time)")

      }

    }
  }

  def overwriteByBTime(clickHouseTable: String, hiveTable: String, hivePartitionBTime: Array[String]): Unit = {
    LOG.warn(s"Overwrite clickHouseTable start", "clickHouseTable", clickHouseTable, "hiveTable", hiveTable, "hivePartitionBTimes", hivePartitionBTime)
    //    taskCount.addAndGet(hivePartitionBTime.length)

    var dataCount = -1L
    if (hosts == null || hosts.size() == 0) {
      throw new NullPointerException("ClickHouse hosts length is 0")
    }

    if (THREAD_POOL == null || THREAD_POOL.isShutdown) {
      THREAD_POOL = ExecutorServiceUtil.createdExecutorService(THREAD_COUNT)
    }
    val countDownLatch = new CountDownLatch(hivePartitionBTime.length)
    val b_dates = hivePartitionBTime.map { eachTime => eachTime.split(" ")(0) }.distinct

    b_dates.foreach { b_date =>
      hivePartitionBTime.foreach { b_time =>
        if (b_date.equals(b_time.split(" ")(0))) {
          THREAD_POOL.execute(new Runnable {
            override def run(): Unit = {
              LOG.warn("ThreadPool", s"start run, b_date=$b_date and b_time=$b_time")
              val time = b_time.replace("-", "_").replace(" ", "__").replace(":", "_")
              val gzDir = s"/root/kairenlo/data-streaming/test/${hiveTable}_${time}_gz.dir"
              var gzFiles = null.asInstanceOf[Array[FileStatus]]

              RunAgainIfError.run {
                // 名称全部转为小写
                val fields = hiveContext
                  .read
                  .table(hiveTable)
                  .schema
                  .fieldNames
                  .map { name =>
                    s"$name as ${name.toLowerCase}"
                  }

                val rows = hiveContext
                  .read
                  .table(hiveTable)
                  .where(s""" b_date = "$b_date" and b_time = "$b_time" """)
                  .selectExpr(fields: _*)

                rows.cache()
                dataCount = rows.count()
                dataCountMap.put(Thread.currentThread().getName, dataCount)

                rows.coalesce(1)
                  .write
                  .mode(SaveMode.Overwrite)
                  .format("json")
                  .option("compression", "gzip")
                  .save(gzDir)
                rows.unpersist()
//                dataCount = hiveContext
//                  .sql(s"""SELECT COUNT(*) FROM $hiveTable WHERE b_time="$b_time"""")
//                  .collect()
//                  .head
//                  .getLong(0)
                LOG.warn(s"dataCount at $b_time is $dataCount")

                LOG.warn(s"gz file saved completed at $gzDir")

                gzFiles = fileSystem.listStatus(new Path(gzDir), new PathFilter {
                  override def accept(path: Path): Boolean = {
                    path.getName.startsWith("part-")
                  }
                })
                // 其实可以多个文件
                if (gzFiles.length > 1) {
                  throw new RuntimeException(s"ClickHouse must upload only one file, but now it finds ${gzFiles.length} in '$gzDir' !!")
                }

                // 验证数据完整性
                gzFiles.foreach { file =>
                  val current = file.getLen
                  val last = lastLengthMap.get(s"$hiveTable^$b_time")
                  if (last != null && current * 1.5 < last) {
                    throw new RuntimeException(s"Hive table '$hiveTable' b_time = '$b_time' reading data are incomplete, less than before.")
                  }
                  lastLengthMap.put(s"$hiveTable^$b_time", current)
                }
              }

              LOG.warn("ClickHouseClient generate gz file completed", gzDir)

              RunAgainIfError.run {
                // 先删除分区
                dropPartition(clickHouseTable, b_date, b_time)

                // 上传到ClickHouse
                gzFiles.foreach { file =>
                  val in = fileSystem.open(file.getPath)
                  LOG.warn(file.getPath.toString)
                  if (uploadToClickHouse(clickHouseTable, hosts(0), in)) {
                    // 上传完成后clickHouse会额外做一些操作，需要等待一小段时间再复制到查询表中
                    Thread.sleep(5 * 1000)
                    var importCount = copyToClickHouseForSearch(clickHouseTable, s"${clickHouseTable}_for_select", b_date, b_time)
                    if (importCount == -1) {
                      throw new HandlerException("Import to select table failed!")
                    }
                    // 如果数据不相等，继续等待一段时间重试
                    var times = 6
                    while (dataCountMap.get(Thread.currentThread().getName) != importCount && times > 0) {
                      LOG.warn(s"dataCount = $dataCount and importCount = $importCount, times left $times")
                      Thread.sleep(5 * 1000)
                      importCount = copyToClickHouseForSearch(clickHouseTable, s"${clickHouseTable}_for_select", b_date, b_time)
                      times -= 1
                    }
                    // 复制完成后删除原表的分区
                    dropPartition(clickHouseTable, b_date, b_time)
                  }
                }
              }
              countDownLatch.countDown()
            }
          })
        }
      }
    }
    countDownLatch.await()
//    countDownLatch.await(5, TimeUnit.MINUTES)
  }


  def dropPartition(clickHouseTable: String, b_date: String, b_time: String): Unit = {
    LOG.warn(s"Start drop partition in table:$clickHouseTable")
    hosts.foreach { host =>
      RunAgainIfError.run {
        val request = new Requests(1)
        request.post(s"http://$host:8123/", new Entity {
          {
            this.setStr(s"ALTER TABLE $clickHouseTable DROP PARTITION ('$b_date', '$b_time')", Entity.CONTENT_TYPE_TEXT)
          }
        }, new Callback {
          override def prepare(conn: HttpURLConnection): Unit = {}

          override def completed(responseStatus: String, response: String): Unit = {}

          override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
            LOG.warn("DROP PARTITION ERROR", s"$responseStatus\n$responseError\n${ex.getMessage} at host $host")
            throw new HandlerException(ex.getMessage)
          }
        }, false)
        LOG.warn(this.getClass.getSimpleName, s"DROP PARTITION($b_date, $b_time) at $host")
      }
    }
  }

  /**
    * 更新至上传表
    */
  def uploadToClickHouse(clickHouseTable: String, host: String, is: InputStream): Boolean = {
    val query = URLEncoder.encode(s"INSERT INTO ${clickHouseTable}_all FORMAT JSONEachRow", "utf-8")
    val conn = new URL(s"http://$host:8123/?enable_http_compression=1&query=$query")
      .openConnection()
      .asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setChunkedStreamingMode(1048576) // 1M
    conn.addRequestProperty("Content-Encoding", "gzip")
    conn.addRequestProperty("Connection", "Keep-Alive")
    conn.setDoInput(true)
    conn.setDoOutput(true)
    val out = conn.getOutputStream
    val buffer = Okio.buffer(Okio.source(is))
    out.write(buffer.readByteArray())
    buffer.close()
    try {
      conn.getInputStream
      return true
      //      val source = Okio.buffer(Okio.source(conn.getInputStream))
      //      val result = source.readString(Charset.forName("utf-8"))
      //      source.close()
      // copy to real table

    } catch {
      case e: Exception =>
        LOG.warn(e.getLocalizedMessage)
        val errorSource = Okio.buffer(Okio.source(conn.getErrorStream))
        val error = errorSource.readString(Charset.forName("UTF-8"))
        errorSource.close()
        throw new HandlerException(error)
    }
    false
  }

  /**
    * 将上传表数据复制到查询表中
    *
    * @param sourceTable 原表(上传表)
    * @param destTable   复制表(查询表)
    */
  def copyToClickHouseForSearch(sourceTable: String, destTable: String, b_date: String, b_time: String): Long = {
    val request = new Requests(1)

    RunAgainIfError.run {
      // 上传出现异常时，可能数据已经上传到clickhouse中，但是异常引发第二次上传，会导致上传两次的情况
      // 上传出现异常时，重新drop partition后再上传
      dropPartition(s"$destTable", b_date, b_time)

      request.post(s"http://${hosts(0)}:8123/", new Entity {
        {
          val sql = s"INSERT INTO ${destTable}_all SELECT * FROM ${sourceTable}_all WHERE b_date='$b_date' AND b_time='$b_time'"
          LOG.warn("sql=", sql)
          this.setStr(sql, Entity.CONTENT_TYPE_TEXT)
        }
      }, new Callback {
        override def prepare(conn: HttpURLConnection): Unit = {}

        override def completed(responseStatus: String, response: String): Unit = {
          LOG.warn(s"copy to clickhouse success")
        }

        override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
          LOG.warn("COPY TO CLICK_HOUSE ERROR", s"$responseStatus\n$responseError\n${ex.getMessage}")
          throw new HandlerException(ex.getMessage)
        }
      }, false)
    }
    // 查询select表中的数据总数
    checkTableCount(destTable, b_date, b_time)
  }

  def checkTableCount(checkTable: String, b_date: String, b_time: String): Long = {
    Thread.sleep(5 * 1000)
    val request = new Requests(1)
    var importCount = -1L
    RunAgainIfError.run {
      request.post(s"http://${hosts(0)}:8123/", new Entity {
        {
          val sql = s"SELECT COUNT(*) FROM ${checkTable}_all WHERE b_date='$b_date' AND b_time='$b_time'"
          LOG.warn("sql=", sql)
          this.setStr(sql, Entity.CONTENT_TYPE_TEXT)
        }
      }, new Callback {
        override def prepare(conn: HttpURLConnection): Unit = {}

        override def completed(responseStatus: String, response: String): Unit = {
          if (response != null) {
            LOG.warn(s"response = $response")
            try {
              val realImportCount = response.trim.toInt
              importCount = realImportCount
            } catch {
              case e: Exception =>
            }
          }
        }

        override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
          LOG.warn("CHECK COUNT ERROR", s"$responseStatus\n$responseError\n${ex.getMessage}")
          throw new HandlerException(ex.getMessage)
        }
      }, false)
    }
    importCount
  }

}
