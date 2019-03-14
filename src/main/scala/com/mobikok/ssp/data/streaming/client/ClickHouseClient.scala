package com.mobikok.ssp.data.streaming.client

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.Charset
import java.util
import java.util.Date
import java.util.concurrent.{CountDownLatch, ExecutorService}

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.{ClickHouseClientException, HandlerException}
import com.mobikok.ssp.data.streaming.util._
import com.mobikok.ssp.data.streaming.util.http.{Callback, Entity, Requests}
import com.typesafe.config.Config
import okio.{Buffer, GzipSink, Okio}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ClickHouseClient(moduleName: String, config: Config, ssc: StreamingContext,
                       messageClient: MessageClient, transactionManager: TransactionManager,
                       hiveContext: HiveContext, moduleTracer: ModuleTracer) extends Transactional {

  private val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
  private val conf = new HdfsConfiguration()
  private val fileSystem = FileSystem.get(conf)

//  private val hosts = config.getStringList("clickhouse.hosts")
  private val hosts = List("master.ck")
  private var aggFields: List[String] = _
  try {
    aggFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map{ c =>
      c.getString("as").toLowerCase()
    }.toList
  } catch {
    case _: Exception =>
      aggFields = List(
        "requestcount",
        "sendcount",
        "showcount",
        "clickcount",
        "feereportcount",
        "feesendcount",
        "feereportprice",
        "feesendprice",
        "cpcbidprice",
        "cpmbidprice",
        "conversion",
        "allconversion",
        "revenue",
        "realrevenue",
        "feecpctimes",
        "feecpmtimes",
        "feecpatimes",
        "feecpasendtimes",
        "feecpcreportprice",
        "feecpmreportprice",
        "feecpareportprice",
        "feecpcsendprice",
        "feecpmsendprice",
        "feecpasendprice",
        "winprice",
        "winnotices",
        "newcount",
        "activecount"
      )
  }
  private var THREAD_POOL = null.asInstanceOf[ExecutorService]
  val THREAD_COUNT = 2
  val dataCountMap = new FixedLinkedMap[String, Long](THREAD_COUNT)

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
          if (uploadToClickHouse(clickHouseTable, hosts.head, in)) {
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

                val groupByFields = (fields.map{ f => f.split(" as ")(1)}.toSet -- aggFields.toSet).toList

                val rows = hiveContext
                  .read
                  .table(hiveTable)
                  .where(s""" b_date = "$b_date" and b_time = "$b_time" """)
                  //.groupBy(groupByFields.head, groupByFields.tail: _*)
                  //.agg(sum(aggFields.head).as(aggFields.head), aggFields.tail.map{ field => sum(field).as(field)}:_*)
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

                LOG.warn(s"dataCount at $b_time is $dataCount")

                LOG.warn(s"gz file saved completed at $gzDir")

                gzFiles = fileSystem.listStatus(new Path(gzDir), new PathFilter {
                  override def accept(path: Path): Boolean = {
                    path.getName.startsWith("part-")
                  }
                })
                rows.unpersist()
                // 最好一个文件
                if (gzFiles.length > 1) {
                  throw new RuntimeException(s"ClickHouse must upload only one file, but now it finds ${gzFiles.length} in '$gzDir' !!")
                }

                // 验证数据完整性
                gzFiles.foreach { file =>
                  val current = file.getLen
                  val last = lastLengthMap.get(s"$hiveTable^$b_time")
                  if (last != null && current * 1.5 < last) {
                    throw new RuntimeException(s"Hive table '$hiveTable' b_time = '$b_time' reading data are incomplete, less than before (before length: $last, current length: $current).")
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
                  if (uploadToClickHouse(clickHouseTable, hosts.head, in)) {
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

  // TODO 临时用于写进月表里
  def hiveWriteToClickHouse(clickHouseTable: String, hiveTable: String, hivePartitionBTime: Array[String]): Unit = {
    LOG.warn(s"clickHouseTable: $clickHouseTable, hiveTable: $hiveTable, hivePartitionBTimes: ${OM.toJOSN(hivePartitionBTime)}")
    //    taskCount.addAndGet(hivePartitionBTime.length)

    var dataCount = -1L
    if (hosts == null || hosts.size() == 0) {
      throw new NullPointerException("ClickHouse hosts length is 0")
    }

    val b_dates = hivePartitionBTime.map { eachTime => eachTime.split(" ")(0) }.distinct

    b_dates.foreach { b_date =>
      hivePartitionBTime.foreach { b_time =>
        if (b_date.equals(b_time.split(" ")(0))) {

          val time = b_time.replace("-", "_").replace(" ", "__").replace(":", "_")
          val gzDir = s"/root/kairenlo/data-streaming/test/${hiveTable}_${time}_gz.dir"
          var gzFiles = null.asInstanceOf[Array[FileStatus]]

          LOG.warn(s"read data from hive at b_date: $b_date, b_time: $b_time")

          RunAgainIfError.run {

            val rows = hiveContext.sql(
              s"""
                |select
                |    coalesce(dwr.publisherId, a.publisherId) as publisherid,
                |    dwr.appId         as appid,
                |    dwr.countryId     as countryid,
                |    dwr.carrierId     as carrierid,
                |    dwr.adType        as adtype,
                |    dwr.campaignId    as campaignid,
                |    dwr.offerId       as offerid,
                |    dwr.imageId       as imageid,
                |    dwr.affSub        as affsub,
                |    dwr.requestCount  as requestcount,
                |    dwr.sendCount     as sendcount,
                |    dwr.showCount     as showcount,
                |    dwr.clickCount    as clickcount,
                |    dwr.feeReportCount as feereportcount,
                |    dwr.feeSendCount   as feesendcount,
                |    dwr.feeReportPrice as feereportprice,
                |    dwr.feeSendPrice  as feesendprice,
                |    dwr.cpcBidPrice   as cpcbidprice,
                |    dwr.cpmBidPrice   as cpmbidprice,
                |    dwr.conversion    as conversion,
                |    dwr.allConversion as allconversion,
                |    dwr.revenue       as revenue,
                |    dwr.realRevenue   as realrevenue,
                |    dwr.b_time,
                |    dwr.l_time,
                |    dwr.b_date,
                |    nvl(p.amId, a_p.amId)      as publisheramid,
                |    nvl(p_am.name, ap_am.name) as publisheramname,
                |    ad.amId           as advertiseramid,
                |    a_am.name         as advertiseramname,
                |    a.mode            as appmodeid,
                |    m.name            as appmodename,
                |    cam.adCategory1   as adcategory1id,
                |    adc.name          as adcategory1name,
                |    cam.name          as campaignname,
                |    cam.adverId       as adverid,
                |    ad.name           as advername,
                |    o.optStatus       as offeroptstatus,
                |    o.name            as offername,
                |    nvl(p.name, a_p.name ) as publishername,
                |    a.name            as appname,
                |    i.iab1name        as iab1name,
                |    i.iab2name        as iab2name,
                |    c.name            as countryname,
                |    ca.name           as carriername,
                |    dwr.adType        as adtypeid,
                |    adt.name          as adtypename,
                |    v.id              as versionid,
                |    dwr.versionName   as versionname,
                |    p_am.proxyId      as publisherproxyid,
                |    cast(null as string)    as data_type,
                |    feeCpcTimes             as feecpctimes,
                |    feeCpmTimes             as feecpmtimes,
                |    feeCpaTimes             as feecpatimes,
                |    feeCpaSendTimes         as feecpasendtimes,
                |    feeCpcReportPrice       as feecpcreportprice,
                |    feeCpmReportPrice       as feecpmreportprice,
                |    feeCpaReportPrice       as feecpareportprice,
                |    feeCpcSendPrice         as feecpcsendprice,
                |    feeCpmSendPrice         as feecpmsendprice,
                |    feeCpaSendPrice         as feecpasendprice,
                |    c.alpha2_code           as countrycode,
                |    dwr.respStatus          as respstatus,
                |    dwr.winPrice            as winprice,
                |    dwr.winNotices          as winnotices,
                |    a.isSecondHighPriceWin  as issecondhighpricewin,
                |    co.id                   as companyid,
                |    co.name                 as companyname,
                |    dwr.test,
                |    dwr.ruleId        as ruleid,
                |    dwr.smartId       as smartid,
                |    pro.id            as proxyid,
                |    s.name            as smartname,
                |    sr.name           as rulename,
                |    co.id             as appcompanyid,
                |    co_o.id           as offercompanyid,
                |    newCount          as newcount,
                |    activeCount       as activecount,
                |    cam.adCategory2   as adcategory2id,
                |    adc2.name         as adcategory2name,
                |    coalesce(p.ampaId, a_p.ampaId)    as publisherampaid,
                |    coalesce(p_amp.name, ap_amp.name) as publisherampaname,
                |    ad.amaaId                         as advertiseramaaid,
                |    a_ama.name                        as advertiseramaaname,
                |    dwr.eventName                     as eventname,
                |    dwr.recommender                   as recommender
                |from ${hiveTable} dwr
                |    left join campaign cam      on cam.id = dwr.campaignId
                |    left join advertiser ad     on ad.id = cam.adverId
                |    left join employee a_am     on a_am.id = ad.amid
                |    left join offer o           on o.id = dwr.offerId
                |    left join publisher p       on p.id = dwr.publisherId
                |    left join employee p_am     on p_am.id = p.amid
                |    left join app a             on a.id = dwr.appId
                |    left join iab i             on i.iab1 = a.iab1 and i.iab2 = a.iab2
                |    left join app_mode m        on m.id = a.mode
                |    left join country c         on c.id = dwr.countryId
                |    left join carrier ca        on ca.id = dwr.carrierId
                |    left join ad_type adt       on adt.id = dwr.adType
                |    left join version_control v on v.version = dwr.versionName
                |    left join ad_category1 adc  on adc.id =  cam.adCategory1
                |    left join ad_category2 adc2 on adc2.id =  cam.adCategory2
                |    left join publisher a_p     on  a_p.id = a.publisherId
                |    left join employee ap_am    on  ap_am.id  = a_p.amId
                |    left join proxy pro         on  pro.id  = ap_am.proxyId
                |    left join company co        on  co.id = pro.companyId
                |    left join other_smart_link s  on s.ID = dwr.smartId
                |    left join smartlink_rules sr  on sr.ID = dwr.ruleId
                |    left join campaign cam_o    on cam_o.id = o.campaignId
                |    left join advertiser ad_o   on ad_o.id = cam_o.adverId
                |    left join employee em_o     on em_o.id = ad_o.amId
                |    left join company co_o      on co_o.id = em_o.companyId
                |    left join employee p_amp    on p_amp.id = p.ampaId
                |    left join employee ap_amp   on  ap_amp.id  = a_p.ampaId
                |    left join employee a_ama    on a_ama.id = ad.amaaId
                |where v.id is not null and b_date='$b_date' and b_time='$b_time'
              """.stripMargin)

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
            LOG.warn(s"dataCount at $b_time is $dataCount")

            LOG.warn(s"gz file saved completed at $gzDir")

            gzFiles = fileSystem.listStatus(new Path(gzDir), new PathFilter {
              override def accept(path: Path): Boolean = {
                path.getName.startsWith("part-")
              }
            })
            // 最好一个文件
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
              if (uploadToClickHouse(clickHouseTable, hosts.head, in)) {
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

        }
      }
    }

  }

  def dropPartition(clickHouseTable: String, b_date: String, b_time: String): Unit = {
    LOG.warn(s"Start drop partition in table:$clickHouseTable")
    hosts.foreach { host =>
      RunAgainIfError.run {
        val request = new Requests(1)
        request.post(s"http://$host:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
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
  def uploadToClickHouse(clickHouseTable: String, host: String, is: InputStream): Boolean = {
    val query = URLEncoder.encode(s"INSERT INTO ${clickHouseTable}_all FORMAT JSONEachRow", "utf-8")
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

      request.post(s"http://${hosts.head}:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
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
      request.post(s"http://${hosts.head}:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8", new Entity {
        {
          val sql = s"SELECT SUM(1) FROM ${checkTable}_all WHERE b_date='$b_date' AND b_time='$b_time'"
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

  /**
    * 这里开始为新增实现事务的方法和变量
    */

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"

  private val uploadTmpTableSign = "_upload_tmp"

  private var dmJoinTableAndExpr: Array[(String, String, String)] = _
  private var dmSelectFields: Array[String] = _
  try {
    dmJoinTableAndExpr = config.getConfigList(s"modules.$moduleName.dm.join.table").map { x =>
      (x.getString("table"), x.getString("as"), x.getString("expr"))
    }.toArray
    dmSelectFields = config.getConfigList(s"modules.$moduleName.dm.join.select").map { x =>
      s"""${x.getString("expr")}  as  ${x.getString("as")}"""
    }.toArray
  } catch {
    case e: Exception =>
  }
  override def init(): Unit = {}

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    try {
      val cleanable = new Cleanable
      if (cookies.isEmpty) {
        val cookieTables = sql(s"show tables like '%$transactionalLegacyDataBackupCompletedTableSign%'", tryAgainWhenError = true)
          .split("[\r]?\n")
          .filter(t => t.length > 0)

        cookieTables.sortBy{ cookieTable => (
          cookieTable.split(transactionalLegacyDataBackupCompletedTableSign)(0)
            .split(TransactionManager.parentTransactionIdSeparator)(0),
          cookieTable.split(transactionalLegacyDataBackupCompletedTableSign)(1)
            .split(TransactionManager.parentTransactionIdSeparator)(1).toInt
        )}.reverse
          .foreach{ cookieTable =>
            val parentId = cookieTable.split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0)
            val tid = cookieTable.split(transactionalLegacyDataBackupCompletedTableSign)(1)

            val needRollBack = transactionManager.isActiveButNotAllCommited(parentId)
            if (needRollBack) {
              LOG.warn("ClickHouse client roll back start")
              val targetTable = s"${cookieTable.split(transactionalLegacyDataBackupCompletedTableSign)(0)}_for_select"

//              val tables = Array(cookieTable)
              val ps = partitions(cookieTable)

              clickHousePartitionAlterSQL(ps).foreach{ partition =>
//                sql(s"alter table $targetTable drop partition($partition)", tryAgainWhenError = true)
                dropPartition(targetTable, partition, hosts:_*)
              }

              val targetFieldSchema = sql(s"desc $targetTable", tryAgainWhenError = true).split("[\r]?\n").map{ field =>
                val schema = field.split("\t")
                (schema(0), schema(1))
              }
              val targetFieldString = targetFieldSchema.map{f => f._1}

              val backupFieldSchema = sql(s"desc $cookieTable", tryAgainWhenError = true).split("[\r]?\n").map{ field =>
                val schema = field.split("\t")
                (schema(0), schema(1))
              }
              val backupFieldString = backupFieldSchema.map{ f => f._1}

              val selects = ListBuffer[String]()
              targetFieldString.zipWithIndex.foreach{ case(field, i) =>
                if (backupFieldString.contains(field)) {
                  selects.append(s"`${targetFieldSchema(i)._1}`")
                } else {
                  if ("String".eq(targetFieldSchema(i)._2)) {
                    selects.append(s"""'' as `${targetFieldSchema(i)._1}`""")
                  } else if (targetFieldSchema(i)._2.startsWith("Int") || targetFieldSchema(i)._2.startsWith("Float")) {
                    // (Int64 or Int32) || (Float64 or Float32)
                    selects.append(s"""0 as ${targetFieldSchema(i)._1}""")
                  } else if (targetFieldSchema(i)._2.startsWith("Date")) {
                    // Date or DateTime(一般不会是这个)
                    selects.append(s"""now() as ${targetFieldSchema(i)._1}""")
                  }
                }
              }

              val batchCount = sql(s"select count(1) from $cookieTable", tryAgainWhenError = true).trim.toInt
              if (batchCount == 0) {
                LOG.warn(s"ClickHouseClient rollback", s"Reverting to the legacy data, Backup table is empty, Drop partitions of targetTable!! \ntable: $targetTable \npartitions: ${partitionsWhereSQL(ps)}")
              } else {
//                var isFirstRun = true
                RunAgainIfError.run {
//                  if (!isFirstRun) {
                    clearRedundantData(ps, targetTable, hosts:_*)
//                  }
//                  isFirstRun = false
                  // 正常情况下只执行一次，防止因为网络问题导致sql执行成功却返回非200，导致数据重复导入的问题
                  sql(s"""insert into ${targetTable}_all select ${selects.mkString(",")} from $cookieTable""")
                }
              }
            }
          }

        sql(s"""show tables like '%$transactionalLegacyDataBackupCompletedTableSign%'""", tryAgainWhenError = true).split("[\r]?\n")
          .filter( t => t.length > 0)
          .sortBy{ x => (
            x.split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0),
            Integer.parseInt(x.split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(1))
          )}
          .reverse
          .foreach{ t => cleanable.addAction(sql(s"drop table if exists $t", tryAgainWhenError = true))}

        sql(s"""show tables like '%$transactionalLegacyDataBackupProgressingTableSign%'""", tryAgainWhenError = true).split("[\r]?\n")
          .filter( t => t.length > 0)
          .foreach{ t => cleanable.addAction(sql(s"drop table if exists $t", tryAgainWhenError = true))}

        sql(s"""show tables like '%$transactionalTmpTableSign%'""", tryAgainWhenError = true).split("[\r]?\n")
          .filter( t => t.length > 0)
          .foreach{ t => cleanable.addAction(sql(s"drop table if exists $t", tryAgainWhenError = true))}

        return cleanable
      }

      val cs = cookies.asInstanceOf[Array[ClickHouseRollbackableTransactionCookie]]
      cs.foreach{ c =>
        val parentId = c.parentId.split(TransactionManager.parentTransactionIdSeparator)(0)

        // 这里获取唯一需要rollback的最新表，后面的返回false
        val needRollBack = transactionManager.isActiveButNotAllCommited(parentId)
        if (needRollBack) {

          LOG.warn("ClickHouse client roll back start")
          clickHousePartitionAlterSQL(c.partitions).foreach{partition =>
//            sql(s"alter table ${c.targetTable} drop partition($partition)", tryAgainWhenError = true)
            dropPartition(s"${c.targetTable}_for_select", partition, hosts: _*)
          }

          val batchCount = sql(s"select count(1) from ${c.transactionalCompletedBackupTable}", tryAgainWhenError = true).trim.toInt
          if (batchCount == 0) {
            LOG.warn(s"ClickHouseClient rollback", s"Reverting to the legacy data, Backup table is empty, Drop partitions of targetTable!! \ntable: ${c.transactionalCompletedBackupTable} \npartitions: ${partitionsWhereSQL(c.partitions)}")
          } else {
            var isFirstRun = true
            RunAgainIfError.run{
              if (!isFirstRun) {
                clearRedundantData(c.partitions, s"${c.targetTable}_for_select", hosts:_*)
              }
              isFirstRun = false
              // 正常情况下只执行一次，防止因为网络问题导致sql执行成功却返回非200，导致数据重复导入的问题
              sql(s"insert into ${c.targetTable}_for_select_all select * from ${c.transactionalCompletedBackupTable}")
            }
          }
        }
      }

      cs.foreach{ c =>
        cleanable.addAction(sql(s"drop table if exists ${c.transactionalCompletedBackupTable}", tryAgainWhenError = true))
        cleanable.addAction(sql(s"drop table if exists ${c.transactionalProgressingBackupTable}", tryAgainWhenError = true))
        cleanable.addAction(sql(s"drop table if exists ${c.transactionalTmpTable}", tryAgainWhenError = true))
      }

      cleanable
    } catch {
      case e: Exception =>
        throw new ClickHouseClientException(s"ClickHouse Transaction Rollback Fail, module: $moduleName, cookies: " + cookies, e)
    }
  }


  /**
    *
    * @param transactionParentId  事务的父ID
    * @param dwrTable             hive table
    * @param clickHouseTable      clickHouse table
    * @param newDF                已经聚合的DataFrame
    * @param partitionField
    * @param partitionFields
    * @return
    */
  def overwriteUnionSum(transactionParentId: String,
                        dwrTable: String,
                        clickHouseTable: String,
                        newDF: DataFrame,
                        partitionField: String,
                        partitionFields: String*): ClickHouseTransactionCookie = {
    val ts = Array(partitionField) ++ partitionFields
    val ps = newDF
      .dropDuplicates(ts)
      .collect()
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }
    overwriteUnionSum(transactionParentId, dwrTable, clickHouseTable, newDF, ps, partitionField, partitionFields:_*)
  }

  def overwriteUnionSum(transactionParentId: String,
                        hiveTable: String,
                        clickHouseTable: String,
                        newDF: DataFrame,
                        ps: Array[Array[HivePartitionPart]],
                        partitionField: String,
                        partitionFields: String*): ClickHouseTransactionCookie = {
    var tid: String = null
    try {
      tid = transactionManager.generateTransactionId(transactionParentId)

//      val dmTable = dwrTable.replace("dwr", "dm")
      if (clickHouseTable == null || clickHouseTable.length == 0) {
        throw new NullPointerException("ClickHouse Table can not be null!")
      }
      val dmTable = clickHouseTable
      val tempTable = dmTable + transactionalTmpTableSign + tid
      val processingTable = dmTable + transactionalLegacyDataBackupProgressingTableSign + tid
      val completeTable = dmTable + transactionalLegacyDataBackupCompletedTableSign + tid

//      val ts = Array(partitionField) ++ partitionFields // l_time, b_date and b_time

      val whereCondition = partitionsWhereSQL(ps) // (l_time="2018-xx-xx xx:00:00" and b_date="2018-xx-xx" and b_time="2018-xx-xx xx:00:00")

      LOG.warn(s"ClickHouseClient overwriteUnionSum overwrite partitions where", whereCondition)

      // after agg fields schema
//      val fs = newDF.schema.fieldNames
//      val fs = sql(s"desc $dmTable", tryAgainWhenError = true).split("[\r]?\n").map{ schema => schema.split("[\t ]")(0) }

//      val aggList = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map{ x => x.getString("as").toLowerCase }
//      val gs = (groupByFields :+ partitionField) ++ partitionFields
//      val gs = fs.filter{ field => !aggList.contains(field) }
//      val unionAggExprsAndAlias = aggList.map{ field => sum(field).as(field) }

//      val _newDF = newDF.select(fs.head, fs.tail: _*)
      moduleTracer.trace("    clickhouse client start overwrite sum")
      // Sum
//      val updated = _newDF
//        .groupBy(gs.head, gs.tail:_*)
//        .agg(
//          unionAggExprsAndAlias.head,
//          unionAggExprsAndAlias.tail:_*
//        )
//        .as("dwr")
//        .select(fs.head, fs.tail:_*)

      val updatedData: Dataset[String] = newDF.toJSON

      moduleTracer.trace("    clickhouse client join finished")

//      LOG.warn(s"update content array length: ${updated.length}, updated take(1):\n${updated.take(1)}")

      // 纯内存可能会OOM
      updatedData.cache()
//      updatedData.persist(StorageLevel.MEMORY_AND_DISK)
      LOG.warn(s"update content array length: ${updatedData.count()}, updated take(1):\n${updatedData.take(1)}")

      if (transactionManager.needTransactionalAction()) {
        sql(s"""CREATE TABLE IF NOT EXISTS $tempTable AS ${dmTable}_for_select""", tryAgainWhenError = true)
        // 事务上传到临时表
        val dmFieldsSchema = sql(s"desc ${dmTable}_for_select").split("[\r]?\n").map{ field => field.split("[\t ]")(0) }
        val dmAggFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map{ x => x.getString("as").toLowerCase() }

        // 重试次数
        var times = 3
        RunAgainIfError.run {

          if (times > 0) {
            sql(s"""CREATE TABLE IF NOT EXISTS $tempTable$uploadTmpTableSign AS $tempTable""", tryAgainWhenError = true)
            val uploadResult = uploadToClickHouseSingleTable(s"$tempTable$uploadTmpTableSign", tempTable, hosts.head, updatedData)
            val dataCount = sql(s"select count(1) from $tempTable$uploadTmpTableSign").trim.toInt

            if (uploadResult && dataCount == updatedData.count()) {
              //            sql(s"INSERT INTO $tempTable SELECT * FROM $tempTable$uploadTmpTableSign UNION ALL SELECT * FROM ${dmTable}_for_select_all WHERE $whereCondition")
              // 每天0点的时候会对一天的数据做聚合，可能会产生clickHouse的内存不够的问题，通过/etc/clickhouse-server/user.xml配置增加内存
              sql(
                s"""INSERT INTO $tempTable
                   |  SELECT ${dmFieldsSchema.map{ f => if (dmAggFields.contains(f)) s"sum($f) as $f" else f}.mkString(", ")}
                   |  FROM
                   |    (SELECT *
                   |    FROM $tempTable$uploadTmpTableSign
                   |    UNION ALL
                   |    SELECT *
                   |    FROM ${dmTable}_for_select_all
                   |    WHERE $whereCondition
                   |    )
                   |  GROUP BY ${dmFieldsSchema.toSet.diff(dmAggFields.toSet).mkString(", ")}"""
                  .stripMargin)
              sql(s"DROP TABLE $tempTable$uploadTmpTableSign")
              updatedData.unpersist()
            } else {
              times -= 1
              sql(s"DROP TABLE $tempTable$uploadTmpTableSign")
              throw new ClickHouseClientException(s"upload to $tempTable$uploadTmpTableSign failed")
            }
          } else {
            val errorMessage = ExceptionUtil.getStackTraceMessage(new RuntimeException("Retry times are more than 5"))
            LOG.warn(s"retry times left 0 times, continue to run\n$errorMessage")
          }

        }
      } else {

        // 重试次数
        var times = 3
        // 非事务直接上传到目标表
        RunAgainIfError.run {
          if (times > 0) {
            sql(s"""create table if not exists $dmTable$uploadTmpTableSign as ${dmTable}_for_select""", tryAgainWhenError = true)
            // 先上传到临时表，再通过insert into select的方式复制
            val uploadResult = uploadToClickHouseSingleTable(s"$dmTable$uploadTmpTableSign", s"${dmTable}_for_select", hosts.head, updatedData)
            val dataCount = sql(s"select count(1) from $dmTable$uploadTmpTableSign").trim.toInt

            if (uploadResult && dataCount == updatedData.count()) {
              sql(s"insert into ${dmTable}_for_select_all select * from $dmTable$uploadTmpTableSign")
              sql(s"drop table $dmTable$uploadTmpTableSign")
              updatedData.unpersist()
            } else {
              sql(s"drop table $dmTable$uploadTmpTableSign")
              times -= 1
              throw new ClickHouseClientException(s"upload to $dmTable$uploadTmpTableSign failed")
            }
          } else {
            val errorMessage = ExceptionUtil.getStackTraceMessage(new RuntimeException("Retry times are more than 5"))
            LOG.warn(s"retry times left 0 times, continue to run\n$errorMessage")

          }

        }

        moduleTracer.trace("    clickhouse client updated to source table finished")
        return new ClickHouseNonTransactionCookie(transactionParentId, tid, dmTable, ps)
      }

      val isE = newDF.take(1).isEmpty
      moduleTracer.trace("    clickhouse client updated to pluggable table finished")
      new ClickHouseRollbackableTransactionCookie(transactionParentId, tid, tempTable, dmTable, SaveMode.Overwrite, ps, processingTable, completeTable, isE)
    } catch {
      case e: Exception =>
        throw new ClickHouseClientException(s"ClickHouse Insert Overwrite Fail, module: $moduleName, transactionId:  $tid", e)
    }
  }

  private def uploadToClickHouseSingleTable(clickHouseTable: String, host: String, data: Array[Byte]): Boolean = {
    val query = URLEncoder.encode(s"INSERT INTO $clickHouseTable FORMAT JSONEachRow", "utf-8")
    val conn = new URL(s"http://$host:8123/?password=9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8&enable_http_compression=1&query=$query")
      .openConnection()
      .asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setChunkedStreamingMode(1048576) // 1M
    conn.setRequestProperty("Content-Encoding", "gzip")
    conn.setRequestProperty("Connection", "Keep-Alive")
    conn.setDoInput(true)
    conn.setDoOutput(true)
    val out = conn.getOutputStream
    out.write(data)
    try {
      conn.getInputStream
      if (conn.getResponseCode == 200) {
        return true
      }

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

  private def uploadToClickHouseSingleTable(targetTable: String, sourceTable: String, host: String, data: Dataset[String]): Boolean = {
//    sql(s"CREATE TABLE IF NOT EXISTS $targetTable as $sourceTable")
    val fileCount = (data.count() / 2000000 + 1).toInt    // 200w条数据为一个文件
    LOG.warn(s"Data count: ${data.count()}, fileCount: $fileCount")
    val dataArray = data.repartition(fileCount).rdd.mapPartitions{ rows =>
      val buffer = new Buffer()
      rows.foreach{ row =>
        buffer.write((row + "\n").getBytes())
      }
//      buffer.write(rows.mkString("\n").getBytes())
      val gzBytes = new ByteArrayOutputStream()
      val gzSink = new GzipSink(Okio.buffer(Okio.sink(gzBytes)))
      gzSink.write(buffer, buffer.size())
      gzSink.flush()
      gzSink.close()

      // 记录gzip文件的长度，4个字节表示
      val length = new Array[Byte](4)
      // 高16位
      length(3) = (gzBytes.size() >> 24).toByte
      length(2) = (gzBytes.size() >> 16 & 0xFF).toByte
      // 低16位
      length(1) = (gzBytes.size() >> 8 & 0xFF).toByte
      length(0) = (gzBytes.size() & 0xFF).toByte
      val gzBytesWithLength = new ByteArrayOutputStream()
      gzBytesWithLength.write(length)
      gzBytesWithLength.write(gzBytes.toByteArray)
      gzBytesWithLength.toByteArray.iterator
    }.collect()

//    LOG.warn(s"Data length: ${dataArray.length} array: ${dataArray.mkString("[", ", ", "]")}")

    val gzFilesArray = new util.ArrayList[ByteArrayOutputStream]()
    var i = 0 // 索引
    while (i < dataArray.length) {
      val gzFileBytes = new ByteArrayOutputStream()
      val fileLength =
        ((dataArray(i + 3).toInt & 0xFF) << 24) |
        ((dataArray(i + 2).toInt & 0xFF) << 16) |
        ((dataArray(i + 1).toInt & 0xFF) << 8)  |
        (dataArray(i).toInt & 0xFF)
      LOG.warn(s"GZFile length: $fileLength, index=$i")
      i += 4
      gzFileBytes.write(dataArray, i, fileLength)
      gzFilesArray.add(gzFileBytes)
//      LOG.warn(s"""GZFile bytes: ${gzFileBytes.toByteArray.mkString("[", ", ", "]")}""")
      i += fileLength
    }
    LOG.warn(s"Total gzFile count: ${gzFilesArray.length}")
    var isFirstRun = true
    RunAgainIfError.run {
      if (!isFirstRun) {
        sql(s"DROP TABLE $targetTable")
        sql(s"CREATE TABLE IF NOT EXISTS $targetTable as $sourceTable")
      }
      isFirstRun = false
      gzFilesArray.par.foreach{ gzFileArray =>
        if (!uploadToClickHouseSingleTable(targetTable, hosts.head, gzFileArray.toByteArray)) {
          throw new ClickHouseClientException(s"Upload to $targetTable failed at thread:${Thread.currentThread().getName}, try again")
        }
      }
    }
    true
  }

  override def commit(cookie: TransactionCookie): Unit = {
    if (cookie.isInstanceOf[ClickHouseNonTransactionCookie]) {
      return
    }

    try {
      val c = cookie.asInstanceOf[ClickHouseRollbackableTransactionCookie]
      if (c.isEmptyData) {
        LOG.warn(s"ClickHouse commit skip, Because no data is inserted (into or overwrite) !!")
      } else {
        sql(s"CREATE TABLE IF NOT EXISTS ${c.transactionalProgressingBackupTable} AS ${c.targetTable}_for_select", tryAgainWhenError = true)

        val bw = c.partitions.map { x => x.filter(y => "l_time".equals(y.name)) }

        //正常情况下只会有一个l_time值
        val flatBw = bw.flatMap { x => x }.toSet
        if (flatBw.size != 1) {
          throw new ClickHouseClientException(s"l_time must be only one value for backup, But has ${flatBw.size} values: $flatBw !")
        }

        LOG.warn(s"ClickHouse before commit backup table ${c.targetTable}_for_select where: ", bw)

        // 只有l_time
        val w = partitionsWhereSQL(bw) // l_time = '201x-xx-xx 00:00:00'
        var isFirstRun = true

        RunAgainIfError.run {
          if (!isFirstRun) {
            sql(s"DROP TABLE ${c.transactionalProgressingBackupTable}", tryAgainWhenError = true)
            sql(s"CREATE TABLE IF NOT EXISTS ${c.transactionalProgressingBackupTable} AS ${c.targetTable}_for_select")
          }
          // 聚合操作(备份当前l_time数据)
//          sql(s"""INSERT INTO ${c.transactionalProgressingBackupTable} SELECT ${dmFieldsSchema.map{ f => if (dmAggFields.contains(f)) s"sum($f) as $f" else f}.mkString(", ")} FROM ${c.targetTable}_for_select_all WHERE $w GROUP BY ${dmFieldsSchema.toSet.diff(dmAggFields.toSet).mkString(", ")}""")
          sql(s"""INSERT INTO ${c.transactionalProgressingBackupTable} SELECT * FROM ${c.targetTable}_for_select_all WHERE $w""")
          isFirstRun = false
        }

        LOG.warn("ClickHouse insert into progressing backup table")
        sql(s"RENAME TABLE ${c.transactionalProgressingBackupTable} TO ${c.transactionalCompletedBackupTable}", tryAgainWhenError = true)
        LOG.warn("ClickHouse rename to completed backup table")


        // 需要先删除分区再进行插入

        RunAgainIfError.run {
          clickHousePartitionAlterSQL(c.partitions).foreach { partition =>
            dropPartition(s"${c.targetTable}_for_select", partition, hosts: _*)
          }
          sql(s"""INSERT INTO ${c.targetTable}_for_select_all SELECT * FROM ${c.transactionalTmpTable}""")
        }

        LOG.warn("ClickHouse insert into target table")
      }
    }
  }

  private def clickHousePartitionAlterSQL(ps: Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.map { y =>
//        "\"" + y.value + "\""
        s"""'${y.value}'"""
        //y._1 + "=" + y._2
      }.mkString(", ")
    }
  }

  private def clearRedundantData(ps: Array[Array[HivePartitionPart]], table: String, hosts: String*): Unit = {
    LOG.warn(s"Run into clear redundant data for table: $table at partition: ${OM.toJOSN(ps)}")
    clickHousePartitionAlterSQL(ps).foreach { partition =>
//      sql(s"alter table $table drop partition($partition)", tryAgainWhenError = true)
      dropPartition(table, partition, hosts:_*)
    }
  }

  private def partitionsWhereSQL(ps: Array[Array[HivePartitionPart]]): String = {
//    [
//    ["l_time": "2018-10-24 10:00:00", "b_date": "2018-10-24", "b_time": "2018-10-24 10:00:00"],
//    ["l_time": "2018-10-24 11:00:00", "b_date": "2018-10-24", "b_time": "2018-10-24 11:00:00"],
//    ["l_time": "2018-10-24 12:00:00", "b_date": "2018-10-24", "b_time": "2018-10-24 12:00:00"],
//    ["l_time": "2018-10-24 13:00:00", "b_date": "2018-10-24", "b_time": "2018-10-24 13:00:00"],
//    ]
//    =>
//    (l_time="2018-10-24 10:00:00" and b_date="2018-10-24" and b_time="2018-10-24 10:00:00") or
//    (l_time="2018-10-24 11:00:00" and b_date="2018-10-24" and b_time="2018-10-24 11:00:00") or
//    (l_time="2018-10-24 12:00:00" and b_date="2018-10-24" and b_time="2018-10-24 12:00:00") or
//    (l_time="2018-10-24 13:00:00" and b_date="2018-10-24" and b_time="2018-10-24 13:00:00")
    var w = ps.map { x =>
      x.map { y =>
        y.name + "=\'" + y.value + "\'"
        //y._1 + "=" + y._2
      }.mkString("(", " and ", ")")
    }.mkString(" or ")
    if ("".equals(w)) w = "1 = 1"
    w
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    try {
      cookies.foreach {
        case c: ClickHouseRollbackableTransactionCookie =>
          sql(s"drop table if exists ${c.transactionalCompletedBackupTable}", tryAgainWhenError = true)
          sql(s"drop table if exists ${c.transactionalProgressingBackupTable}", tryAgainWhenError = true)
          sql(s"drop table if exists ${c.transactionalTmpTable}", tryAgainWhenError = true)
        //          sql(s"drop table if exists ${c.transactionalTmpTable}$uploadTmpTableSign", tryAgainWhenError = true)
        case other =>
          LOG.warn(other.getClass.getName)
      }
    } catch {
      case e: Exception =>
        throw new ClickHouseClientException(s"ClickHouse Transaction Clean Fail, module: $moduleName, cookie: " + OM.toJOSN(cookies), e)
    }
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
    LOG.warn(s"execute clickhouse sql:\n$sql")
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
          throw new ClickHouseClientException(s"sql may be failed:\n$sql")
        }

        override def completed(responseStatus: String, response: String): Unit = {
          result = response
        }
      }, false)
    result
  }
}
