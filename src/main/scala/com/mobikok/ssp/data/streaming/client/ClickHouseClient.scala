package com.mobikok.ssp.data.streaming.client

import java.io.InputStream
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.Charset
import java.util.Date
import java.util.concurrent.{CountDownLatch, ExecutorService}

import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.{ClickHouseClientException, HandlerException}
import com.mobikok.ssp.data.streaming.transaction.TransactionManager
import com.mobikok.ssp.data.streaming.util._
import com.mobikok.ssp.data.streaming.util.http.{Callback, Entity, Requests}
import com.typesafe.config.Config
import okio.Okio
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.http.HttpStatus
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class ClickHouseClient(moduleName: String, config: Config, ssc: StreamingContext,
                       messageClient: MessageClient, transactionManager: TransactionManager,
                       hiveContext: HiveContext, moduleTracer: ModuleTracer) {

  private val LOG: Logger = new Logger(moduleName, getClass, new Date().getTime)
  private val conf = new HdfsConfiguration()
  private val fileSystem = FileSystem.get(conf)

  private val hosts = if(config.hasPath("clickhouse.hosts")) config.getStringList("clickhouse.hosts").asScala else List()

  private var THREAD_POOL = null.asInstanceOf[ExecutorService]
  val THREAD_COUNT = 3
  val dataCountMap = new FixedLinkedMap[String, Long](THREAD_COUNT)

  def overwriteByBTime(clickHouseTmpTable: String, hiveTable: String, hivePartitionBTimes: Array[String]): Unit = {
    LOG.warn(s"ClickHouseTable all b_time overwrite start", "clickHouseTable", clickHouseTmpTable, "hiveTable", hiveTable, "hivePartitionBTimes", hivePartitionBTimes)
    moduleTracer.trace(s"     ck overwrite b_time(s): ${hivePartitionBTimes.mkString("\n                                                ", "\n                                                ", "")}")
    //    taskCount.addAndGet(hivePartitionBTime.length)

//    var dataCount = -1L
    if (hosts == null || hosts.size() == 0) {
      throw new NullPointerException("ClickHouse hosts length is 0")
    }

    if (THREAD_POOL == null || THREAD_POOL.isShutdown) {
      THREAD_POOL = ExecutorServiceUtil.createdExecutorService(THREAD_COUNT)
    }
    val countDownLatch = new CountDownLatch(hivePartitionBTimes.length)
    val b_dates = hivePartitionBTimes.map { eachTime => eachTime.split(" ")(0) }.distinct

    b_dates.foreach { b_date =>
      hivePartitionBTimes.foreach { b_time =>
        if (b_date.equals(b_time.split(" ")(0))) {
          THREAD_POOL.execute(new Runnable {
            override def run(): Unit = {
              LOG.warn(s"ClickHouseTable a b_time overwrite start", "clickHouseTable", clickHouseTmpTable, s"b_time", b_time, "hiveTable", hiveTable)
              val time = b_time.replace("-", "_").replace(" ", "__").replace(":", "_")
              val gzDir = s"/root/kairenlo/data-streaming/test/${hiveTable}_${time}_gz.dir"
              var gzFiles = null.asInstanceOf[Array[FileStatus]]

              var hiveDataCount = 0L
              var rows: DataFrame = null
              RunAgainIfError.run({
                // 名称全部转为小写
                val fields = hiveContext
                  .read
                  .table(hiveTable)
                  .schema
                  .fieldNames
                  .map { name =>
                    s"$name as ${name.toLowerCase}"
                  }

//                val groupByFields = (fields.map{ f => f.split(" as ")(1)}.toSet -- aggFields.toSet).toList

                rows = hiveContext
                  .read
                  .table(hiveTable)
                  .where(s""" b_date = "$b_date" and b_time = "$b_time" """)
                  //.groupBy(groupByFields.head, groupByFields.tail: _*)
                  //.agg(sum(aggFields.head).as(aggFields.head), aggFields.tail.map{ field => sum(field).as(field)}:_*)
                  .selectExpr(fields: _*)

                rows.cache()
                hiveDataCount = rows.count()
//                dataCountMap.put(Thread.currentThread().getId+"", hiveDataCount)

                rows.repartition(1)//coalesce(1)
                  .write
                  .mode(SaveMode.Overwrite)
                  .format("json")
                  .option("compression", "gzip")
                  .save(gzDir)

                LOG.warn(s"dataCount at $b_time is $hiveDataCount")

                LOG.warn(s"gz file saved completed at $gzDir")

                gzFiles = fileSystem.listStatus(new Path(gzDir), new PathFilter {
                  override def accept(path: Path): Boolean = {
                    path.getName.startsWith("part-")
                  }
                })
                rows.unpersist()
                // 确保是一个文件
                if (gzFiles.length > 1) {
                  throw new RuntimeException(s"ClickHouse must upload only one file, but now it finds ${gzFiles.length} in '$gzDir' !!")
                }

              }, {_:Throwable=>
                if(rows != null) rows.unpersist()
              })

              LOG.warn("ClickHouseClient generate gz file completed", gzDir)

              RunAgainIfError.run {
                // 先删除临时表分区，确保临时表分区无数据
                dropPartition(clickHouseTmpTable, b_date, b_time)

                // 上传到ClickHouse
                gzFiles.foreach { file =>
                  val in = fileSystem.open(file.getPath)
                  LOG.warn("File path", file.getPath.toString)
                  uploadToClickHouse(clickHouseTmpTable, b_date, b_time, hosts.head, in, hiveDataCount)
                  copyToClickHouseForSearch(clickHouseTmpTable, s"${clickHouseTmpTable}_for_select", b_date, b_time, hiveDataCount)
                }

                // 复制完成后删除临时表的分区
                //dropPartition(clickHouseTmpTable, b_date, b_time)

                //清理文件
                gzFiles.foreach{x=>
                  fileSystem.delete(x.getPath, true)
                }

              }
              countDownLatch.countDown()
              LOG.warn(s"ClickhouseTable a b_time overwrite done",  "clickHouseTable", clickHouseTmpTable,"b_time", b_time, "hiveTable", hiveTable)
            }
          })
        }
      }
    }
    countDownLatch.await()
    LOG.warn(s"ClickHouseTable all b_time overwrite done", "clickHouseTable", clickHouseTmpTable, "hiveTable", hiveTable, "hivePartitionBTimes", hivePartitionBTimes)
    //    countDownLatch.await(5, TimeUnit.MINUTES)
  }
  def createTableIfNotExists(table: String, like: String): Unit = {
    sql(s"create table if not exists $table as $like")
  }
  def createTableWithEngineIfNotExists(table: String, like: String): Unit = {
    sql(s"create table if not exists $table as $like ENGINE = Distributed(bip_ck_cluster, default, $like, rand())")
  }

  def dropPartition(clickHouseBaseTable: String, b_date: String, b_time: String): Unit = {
    LOG.warn(s"Drop partition at all host start", "table", clickHouseBaseTable, "b_time", b_time, "hosts", hosts)
    hosts.foreach { host =>
      RunAgainIfError.run {

        LOG.warn(this.getClass.getSimpleName, s"ALTER TABLE $clickHouseBaseTable DROP PARTITION ('$b_date', '$b_time') at $host START")

        val request = new Requests(1)
        request.post(s"http://$host:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
          {
            this.setStr(s"ALTER TABLE $clickHouseBaseTable DROP PARTITION ('$b_date', '$b_time')", Entity.CONTENT_TYPE_TEXT)
          }
        }, new Callback {
          override def prepare(conn: HttpURLConnection): Unit = {}

          override def completed(responseStatus: String, response: String): Unit = {
            if(!responseStatus.startsWith(HttpStatus.SC_OK + "")) {
              LOG.warn("DROP PARTITION ERROR", s"$responseStatus\n$response\n at host $host")
              throw new HandlerException(response)
            }
          }

          override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
            LOG.warn("DROP PARTITION ERROR", s"$responseStatus\n$responseError\n${ex.getMessage} at host $host")
            throw new HandlerException(ex.getMessage)
          }
        }, false)
        LOG.warn(this.getClass.getSimpleName, s"ALTER TABLE $clickHouseBaseTable DROP PARTITION ('$b_date', '$b_time') at $host DONE")
      }
    }

    // 确保数据清零了
    awaitCheckClickhouseTableCount(clickHouseBaseTable, b_date, b_time, 0, "drop partition")

    LOG.warn(s"Drop partition all host done", "table", clickHouseBaseTable, "b_time", b_time, "hosts", hosts)

  }

  def dropPartition(clickHouseTable: String, partition: String, hosts: String*): Unit = {
    LOG.warn(s"Start drop partition in table:$clickHouseTable")
    hosts.foreach { host =>
      RunAgainIfError.run {
        val request = new Requests(1)
        request.post(s"http://$host:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
          {
            this.setStr(s"ALTER TABLE $clickHouseTable DROP PARTITION ($partition)", Entity.CONTENT_TYPE_TEXT)
          }
        }, new Callback {
          override def prepare(conn: HttpURLConnection): Unit = {}

          override def completed(responseStatus: String, response: String): Unit = {}

          override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
            LOG.warn("DROP PARTITION ERROR", s"$responseStatus\n$responseError\n${ex.getMessage} at host $host")
            throw new HandlerException(ex.getMessage)
          }
        }, false)
        LOG.warn(this.getClass.getSimpleName, s"DROP PARTITION($partition) at $host")
      }
    }
  }

  /**
    * 更新至上传表
    */
  def uploadToClickHouse(clickHouseBaseTable: String, b_date: String, b_time: String, host: String, is: InputStream, hiveDataCount: Long):Unit = {
    val query = URLEncoder.encode(s"INSERT INTO ${clickHouseBaseTable}_all FORMAT JSONEachRow", "utf-8")
    val conn = new URL(s"http://$host:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8&enable_http_compression=1&query=$query")
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
      awaitCheckClickhouseTableCount(clickHouseBaseTable, b_date, b_time, hiveDataCount, "upload to tmp table")
      //      val source = Okio.buffer(Okio.source(conn.getInputStream))
      //      val result = source.readString(Charset.forName("utf-8"))
      //      source.close()

    } catch {
      case e: Exception =>
        LOG.warn(e.getLocalizedMessage)
        val errorSource = Okio.buffer(Okio.source(conn.getErrorStream))
        val error = errorSource.readString(Charset.forName("UTF-8"))
        errorSource.close()
        throw new ClickHouseClientException(error)
    }
  }

  /**
    * 将上传表数据复制到查询表中
    *
    * @param sourceTmpTable 原表(上传表)
    * @param destBaseTable   复制表(查询表)
    */
  def copyToClickHouseForSearch(sourceTmpTable: String, destBaseTable: String, b_date: String, b_time: String, hiveDataCount: Long): Unit = {
    val request = new Requests(1)

    RunAgainIfError.run {
      // 上传出现异常时，可能数据已经上传到clickhouse中，但是异常引发第二次上传，会导致上传两次的情况
      // 上传出现异常时，重新drop partition后再上传
      dropPartition(s"$destBaseTable", b_date, b_time)

      request.post(s"http://${hosts.head}:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
        {
          val sql = s"INSERT INTO ${destBaseTable}_all SELECT * FROM ${sourceTmpTable}_all WHERE b_date='$b_date' AND b_time='$b_time'"
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
//    // 查询select表中的数据总数
//    queryTableCount(destTable, b_date, b_time)
    awaitCheckClickhouseTableCount(destBaseTable, b_date, b_time, hiveDataCount, "copy to clickHouse select table")

  }

  def awaitCheckClickhouseTableCount(clickHouseBaseTable: String, b_date: String, b_time: String, expectedCount: Long, logInfo: String): Unit ={

    var ckTableCount = 0L
    var times = 0L
    var b = true
    while(b){
      ckTableCount = queryTableCount(clickHouseBaseTable, b_date, b_time)
      b = expectedCount != ckTableCount
      times += 1
      LOG.warn(s"Polling queryTableCount on $logInfo", "clickHouseTable", clickHouseBaseTable, "b_date", b_date,"b_time", b_time, "ckTableCount", ckTableCount, "expectedCount", expectedCount, "checkTimes", times)
      if(b) {
        Thread.sleep(3*1000L)
      }
    }

  }

  def queryTableCount(clickHouseBaseTable: String, b_date: String, b_time: String): Long = {
//    Thread.sleep(5 * 1000)
    val request = new Requests(1)
    var importCount = 0L
    RunAgainIfError.run {
      request.post(s"http://${hosts.head}:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
        {
          val sql = s"SELECT SUM(1) FROM ${clickHouseBaseTable}_all WHERE b_date='$b_date' AND b_time='$b_time'"
          LOG.warn("sql=", sql)
          this.setStr(sql, Entity.CONTENT_TYPE_TEXT)
        }
      }, new Callback {
        override def prepare(conn: HttpURLConnection): Unit = {}

        override def completed(responseStatus: String, response: String): Unit = {
          if (response != null) {
            LOG.warn(s"Exec sql result", response)
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

  def partitions(tableNames:String*): Array[Array[HivePartitionPart]] ={
    val ps = mutable.ListBuffer[Array[HivePartitionPart]]()
    tableNames.foreach{ tableName =>
      sql(s"SELECT partition FROM system.parts WHERE table='$tableName' and active=1", tryAgainWhenError = true).split("[\r]?\n")
        .filter(line => line.length > 0)
        .foreach{ partition =>
          ps += parseShowPartition(partition)
        }
    }
    ps.toArray[Array[HivePartitionPart]]
  }

  def parseShowPartition (partition: String): Array[HivePartitionPart] = {
    val part = partition.replaceAll("[\\\\\\'()]", "").split(",").map{ p => p.trim }
    if (part.length != 3) {
      throw new ClickHouseClientException("partition must be (l_time, b_date, b_time)")
    }
    val partitionFields = Array("l_time", "b_date", "b_time")
    part.zipWithIndex.map{ case(each, i) =>
      HivePartitionPart(partitionFields(i), each)
    }
  }

  def sql(sql: String, tryAgainWhenError: Boolean = false): String = {
    LOG.warn(s"execute sql", sql)
    if (tryAgainWhenError) {
      RunAgainIfError.run{
        sendSQLToClickHouse(sql)
      }
    } else {
      sendSQLToClickHouse(sql)
    }
  }

  val request = new Requests(1)
  def sendSQLToClickHouse(sql: String): String ={
    var result = ""
    request.post(s"http://${hosts.head}:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity().setStr(sql, Entity.CONTENT_TYPE_TEXT),
      new Callback {
        override def prepare(conn: HttpURLConnection): Unit = {}

        override def failed(responseStatus: String, responseError: String, ex: Exception): Unit = {
          LOG.warn(ex.getLocalizedMessage)
          throw new ClickHouseClientException(s"Sql '$sql' execute failed, Error: $responseError", ex)
        }

        override def completed(responseStatus: String, response: String): Unit = {
          result = response
        }
      }, false)
    result
  }
}
