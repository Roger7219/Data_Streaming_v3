package com.mobikok.ssp.data.streaming.client

import java.io.{FileInputStream, InputStream}
import java.nio.channels.Channels
import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch
import javax.management.RuntimeErrorException

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.BigQuery.TableListOption
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import com.mobikok.message.client.MessageClient
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.google.cloud.bigquery.TimePartitioning
import com.google.cloud.bigquery.TimePartitioning.Type
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util._
import org.apache.spark.sql.types._
/**
  * Created by Administrator on 2017/10/19.
  */
class BigQueryClient (moduleName: String, config: Config, ssc: StreamingContext, messageClient: MessageClient, hiveContext:HiveContext) {

  private val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
  val reportDataset = "report_dataset"
  val conf = new HdfsConfiguration()
  val fileSystem = FileSystem.get(conf)

  import com.google.cloud.bigquery.BigQueryOptions
  import com.google.cloud.http.HttpTransportOptions

  val transportOptions: HttpTransportOptions = BigQueryOptions.getDefaultHttpTransportOptions.toBuilder().setConnectTimeout(600000).setReadTimeout(600000).build();
  val bigquery: BigQuery = BigQueryOptions.newBuilder
    .setProjectId("dogwood-seeker-182806")
    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("key.json")))
    .setTransportOptions(transportOptions)
    .build
    .getService

  var bigQueryJDBCClient = new BigQueryJDBCClient("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath=/usr/bigquery/key.json;Timeout=3600;")

  def createTableLikeHive (bigQueryTable: String, hiveTable: String): Unit = {
    createTableLikeHive(bigQueryTable, hiveTable, true)
  }

  def createTableLikeHive (bigQueryTable: String, hiveTable: String,  isPartitionTable:Boolean): Unit = {

    LOG.warn("Create BigQuery table start", s"bigQueryTable", bigQueryTable, "hiveTable", hiveTable, "isPartitionTable", isPartitionTable)

    val fs = hiveContext.read.table(hiveTable).schema.fields.map{x=>
      val dt = x.dataType

      if(dt.isInstanceOf[StringType]) {
        Field.of(x.name,  Field.Type.string)
      } else if(dt.isInstanceOf[IntegerType] || dt.isInstanceOf[LongType]){
        Field.of(x.name,  Field.Type.integer())
      } else if(dt.isInstanceOf[FloatType] || dt.isInstanceOf[DoubleType] || dt.isInstanceOf[DecimalType]) {
        Field.of(x.name,  Field.Type.floatingPoint())
      } else {
        throw new HandlerException(s"StructField type '${x.dataType.typeName}' of field '${x.name}' is not supported, Only supports type: string, integer, long, float, double and decimal")
      }
    }
    val schema = Schema.of(fs:_*)
    val tableId = TableId.of(reportDataset, bigQueryTable)

    val partitioning = TimePartitioning.of(Type.DAY)
    var tableDefinitionBuilder = StandardTableDefinition
      .newBuilder
      .setSchema(schema)

    if(isPartitionTable) {
      tableDefinitionBuilder = tableDefinitionBuilder.setTimePartitioning(partitioning)
    }

    var err: Exception = null
    var createdTable: Table = null
    try {
      createdTable = bigquery.create(TableInfo.of(tableId, tableDefinitionBuilder.build()))
    }catch {case e: Exception =>
      err = e;
    }

    if(err == null) {
      LOG.warn("Create BigQuery done", "table", String.valueOf(createdTable))
    }else {
      if(String.valueOf(err.getMessage).contains("Already Exists")) {
        LOG.warn(s"Create BigQuery table fail( already exists )", "table", String.valueOf(createdTable))
      }else {
        LOG.warn(s"Create BigQuery table fail", "table", String.valueOf(createdTable), "exception", err)
        throw err;
      }
    }


  }

  // Copy方式分区表覆盖
  def overwriteByBDate(bigQueryTable: String, hiveTable: String, hivePartitionBDate: String): Unit = {


    LOG.warn(s"Overwrite bigQueryTable start", "bigQueryTable", bigQueryTable, "hiveTable", hiveTable, "b_date", hivePartitionBDate)

    var suffix = df.format(new Date())

    RunAgainIfError.run({

      //可能是没有建表，尝试建表
      try {
        LOG.warn("Try build table if not exists like hive table", "table", bigQueryTable)
        createTableLikeHive(bigQueryTable, hiveTable)
      }catch {case e=>
        if(!String.valueOf(e.getMessage).contains("Already Exists")) {
          LOG.warn(s"Create BigQuery table '$bigQueryTable' fail", e)
          throw e
        }
      }

      var b_date = hivePartitionBDate

      var forUploadT = forUpdateTmpTableName(bigQueryTable, b_date, suffix)//s"$bigQueryTable_${x.replaceAll("-", "_")}_for_update"
      var bqPartitionDay = b_date.replaceAll("-", "")
      try{
        createTableLikeHive(forUploadT, hiveTable, false)
        LOG.warn("Create BigQuery table if not exists done", "table", forUploadT, "isPartitionTable", false)
      }catch {case e:Exception=>
        if(!String.valueOf(e.getMessage).contains("Already Exists")) {
          LOG.warn(s"Create BigQuery table '$forUploadT' fail", e)
        }
      }

      var fn = b_date.replaceAll("-","_")
      var gzDir = s"/root/kairenlo/pluggable/${hiveTable}_${fn}_gz.dir"

      val hiveContext = new HiveContext(ssc.sparkContext)

      LOG.warn("BigQueryClient generate gz file starting", gzDir)
      hiveContext
        .read
        .table(hiveTable)
        .where(s""" b_date = "$b_date" """)
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("json")
        .option("compression", "gzip")
        //      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(gzDir)
      LOG.warn("BigQueryClient generate gz file completed", gzDir)

      //Must Only one file
      val gz = fileSystem
        .listStatus(new Path(gzDir), new PathFilter {
          override def accept (path: Path): Boolean = {
            path.getName.startsWith("part-")
          }
        })
      if (gz.size > 1) {
        throw new RuntimeException(s"BigQuery must upload only one file, but now it finds ${gz.size} in '${gzDir}' !!")
      }

      gz.foreach{f=>
        uploadToBigQueryGeneralTable(forUploadT, fileSystem.open(f.getPath), WriteDisposition.WRITE_TRUNCATE)
      }

      copyTable(s"$bigQueryTable$$$bqPartitionDay", s"$forUploadT", hiveTable , bqPartitionDay, b_date)

      bigquery.delete(TableId.of(reportDataset, forUploadT))
//      }
    })

    LOG.warn(s"Overwrite bigQueryTable done", "bigQueryTable", bigQueryTable, "hiveTable", hiveTable, "b_date", hivePartitionBDate)

  }

  /**
    * 早期的按天上传
    * @param hivePartitionBDate eg: 2017-10-19
    */
  def overwrite (bigQueryTable: String, hiveTable: String, hivePartitionBDate: String): Unit = {
    var bqDay = hivePartitionBDate.split(" ")(0).replaceAll("-", "")

    LOG.warn(s"Overwrite bigQueryTable start", "bigQueryTable", bigQueryTable, "hiveTable", hiveTable, "hivePartitionBDate", hivePartitionBDate, "bqPartitionDay", bqDay)

    var gzDir = s"/root/kairenlo/pluggable/${hiveTable}_${hivePartitionBDate}_gz.dir"

    val hiveContext = new HiveContext(ssc.sparkContext)

    LOG.warn("BigQueryClient generate gz file starting", gzDir)
    hiveContext
      .read
      .table(hiveTable)
      .where(s""" b_date = "$hivePartitionBDate" """)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .option("compression", "gzip")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(gzDir)

    LOG.warn("BigQueryClient generate gz file completed", gzDir)

    //Must Only one file
    val gz = fileSystem
      .listStatus(new Path(gzDir), new PathFilter {
        override def accept (path: Path): Boolean = {
          path.getName.startsWith("part-")
        }
      })

    if (gz.size > 1) {
      throw new RuntimeException(s"Bigquery must upload only one file, but now it finds ${gz.size} in '${gzDir}' !!")
    }
    gz.foreach { f =>

      try {

        uploadToBigQueryPartitionTable(bigQueryTable, fileSystem.open(f.getPath), bqDay, WriteDisposition.WRITE_TRUNCATE)

      }catch {case e:BigQueryException =>
        if(ExceptionUtils.getStackTrace(e).indexOf("Partitioning specification must be provided in order to create partitioned table") >= 0) {

          LOG.warn("BigQuery try to build table like hive table, catch exception", e.getMessage)
          createTableLikeHive(bigQueryTable, hiveTable)

          //try again
          uploadToBigQueryPartitionTable(bigQueryTable, fileSystem.open(f.getPath), bqDay, WriteDisposition.WRITE_TRUNCATE)

        }else {
          throw e
        }

      }
    }

    LOG.warn(s"Overwrite bigQueryTable done", "bigQueryTable", bigQueryTable, "hiveTable", hiveTable, "hivePartitionBDate", hivePartitionBDate, "bqPartitionDay", bqDay)
  }



  private def uploadToBigQueryPartitionTable (bigQueryTable: String, in: InputStream, bqPartitionDay: String, writeDisposition: WriteDisposition): Unit = {
    uploadToBigQuery0(bigQueryTable, in, bqPartitionDay, null, writeDisposition/*WriteDisposition.WRITE_TRUNCATE*/)
  }


  private def uploadToBigQueryGeneralTable (bigQueryTable: String, in: InputStream, writeDisposition: WriteDisposition): Unit = {
    uploadToBigQuery0(bigQueryTable, in, null, null, writeDisposition/*WriteDisposition.WRITE_APPEND*/)
  }

  private def uploadToBigQueryGeneralTable (bigQueryTable: String, in: InputStream, b_time: String, writeDisposition: WriteDisposition): Unit = {
    uploadToBigQuery0(bigQueryTable, in, null, b_time, writeDisposition/*WriteDisposition.WRITE_APPEND*/)
  }


  private def uploadToBigQuery0 (bigQueryTable: String, in: InputStream, bqPartitionDay: String, hivePartitionBTime:String, writeDisposition:WriteDisposition): Unit = {

    var t = bigQueryTable
    if(StringUtil.notEmpty(bqPartitionDay)) {
      t =  s"$bigQueryTable$$$bqPartitionDay"
    }
    LOG.warn("BigQuery upload starting", "table", t,  "b_time", hivePartitionBTime, "bqDay", bqPartitionDay, "writeDisposition", writeDisposition)


    val tableId = TableId.of(reportDataset, t)
    val writeChannelConfiguration = WriteChannelConfiguration
      .newBuilder(tableId)
      .setFormatOptions(FormatOptions.json())
      .setWriteDisposition(writeDisposition)
      .build()
    val writer = bigquery.writer(writeChannelConfiguration)
    val out = Channels.newOutputStream(writer)

    try {
      IOUtils.copy(in, out)
    }finally {
      IOUtils.closeQuietly(out)
      IOUtils.closeQuietly(in)
    }

    // Get load job
    var job = writer.getJob()
    job = job.waitFor()
    val stats = job.getStatistics[JobStatistics]()
    LOG.warn("BigQuery upload done !!", "table", bigQueryTable, "bqDay", bqPartitionDay, "b_time", hivePartitionBTime, "stats", stats, "status", job.getStatus)

    if(job.getStatus.getError != null ){
      throw new Error("BigQuery upload fail: " + job.getStatus )
    }

  }

  var TABLE_FOR_UPDATE_SUFFIX = "for_update"
  var df = CSTTime.formatter("yyyyMMddHHmmss")

  def forUpdateTmpTableName (bigQueryTable: String, b_date: String, suffix:String): String ={
    return s"${forUpdateTmpTableNamePrefix(bigQueryTable, b_date)}__${suffix}__$TABLE_FOR_UPDATE_SUFFIX"
  }

  // if table name is ssp_report_overall_dm__20180503__20180503054351__for_update,
  // then prefix: ssp_report_overall_dm__20180503
  def forUpdateTmpTableNamePrefix (bigQueryTable: String, b_date: String): String ={
    return s"${targetTableNameWithSuffixForUpadte(bigQueryTable)}${b_date.replaceAll("-", "")}"
  }

  def targetTableNameWithSuffixForUpadte(bigQueryTable: String): String ={
    return s"${bigQueryTable}___${moduleName}___"
  }

  private var executorService = ExecutorServiceUtil.createdExecutorService(5)// must is 1

  // 按b_time更新，基本是小时 new !!
  def overwriteByBTime (bigQueryPartitionedTable: String, hiveTable: String, hivePartitionBTimes: Array[String]): Unit = {

    LOG.warn(s"Overwrite bigQueryTable start", "bigQueryTable", bigQueryPartitionedTable, "hiveTable", hiveTable, "hivePartitionBTimes", hivePartitionBTimes)

    RunAgainIfError.run({

      var suffix = df.format(new Date())

      //可能是没有建表，尝试建表
      LOG.warn("Try build table if not exists like hive table", "table", bigQueryPartitionedTable)
      createTableLikeHive(bigQueryPartitionedTable, hiveTable)

      val b_dates = hivePartitionBTimes.map{x=>x.split(" ")(0)}.distinct

      b_dates.foreach{ b_date=>
        var forUploadT = forUpdateTmpTableName(bigQueryPartitionedTable, b_date, suffix)//s"$bigQueryTable_${x.replaceAll("-", "_")}_for_update"
        var bqPartitionDay = b_date.replaceAll("-", "")
        createTableLikeHive(forUploadT, hiveTable, false)

        copyTable(s"$forUploadT" , s"$bigQueryPartitionedTable$$$bqPartitionDay", hiveTable , bqPartitionDay, b_date)

        hivePartitionBTimes.foreach{ b_time=>

          if(b_date.equals(b_time.split(" ")(0))) {

            var fn = b_time.replaceAll("-","_").replaceAll(" ", "__").replaceAll(":", "_")
            var gzDir = s"/root/kairenlo/pluggable/${hiveTable}_${fn}_gz.dir"
            var gz: Array[FileStatus] = null

            val hiveContext = new HiveContext(ssc.sparkContext)

            LOG.warn("BigQueryClient generate gz file starting", gzDir)

            RunAgainIfError.run{
              hiveContext
                .read
                .table(hiveTable)
                .where(s""" b_date = "$b_date" and b_time = "$b_time" """)
                .repartition(1)
                .write
                .mode(SaveMode.Overwrite)
                .format("json")
                .option("compression", "gzip")
                //      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .save(gzDir)

              //Must Only one file
              gz = fileSystem
                .listStatus(new Path(gzDir), new PathFilter {
                  override def accept (path: Path): Boolean = {
                    path.getName.startsWith("part-")
                  }
                })

              //Must Only one file
              if (gz.size > 1) {
                throw new RuntimeException(s"BigQuery must upload only one file, but now it finds ${gz.size} in '${gzDir}' !!")
              }

              //验证数据完整性
              gz.foreach{f=>
                val curr = f.getLen
                var last = lastLengthMap.get(s"$hiveTable^$b_time")
                if(last != null && curr*1.5 < last) {
                  throw new RuntimeException(s"Hive table '$hiveTable' b_time = '$b_time' reading data are incomplete, less than before (before length: $last, current length: ${curr}).")
                }
                lastLengthMap.put(s"$hiveTable^$b_time", curr)
              }

            }

            LOG.warn("BigQueryClient generate gz file completed", gzDir)

            gz.foreach{f=>
//              val b_date = b_time.split(" ")(0)
//              var uploadT = forUpdateTmpTableName(bigQueryPartitionedTable, b_date, suffix)
//              var bqPartitionDay = b_date.replaceAll("-", "")

              var bigQueryJDBCClient = new BigQueryJDBCClient("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath=/usr/bigquery/key.json;Timeout=3600;")

              bigQueryJDBCClient.execute(s"delete from ${reportDataset}.$forUploadT where b_time = '$b_time'")
              uploadToBigQueryGeneralTable(forUploadT, fileSystem.open(f.getPath), b_time, WriteDisposition.WRITE_APPEND)
              copyTable(s"$bigQueryPartitionedTable$$$bqPartitionDay", s"$forUploadT", hiveTable , bqPartitionDay, b_date)
            }
          }
        }

//        copyTable(s"$bigQueryPartitionedTable$$$bqPartitionDay", s"$forUploadT", hiveTable , bqPartitionDay, b_date)

//        deleteTable(forUpdateTmpTableName(bigQueryPartitionedTable, b_date, suffix))
      }
      deleteTables(targetTableNameWithSuffixForUpadte(bigQueryPartitionedTable), TABLE_FOR_UPDATE_SUFFIX)

    })

    LOG.warn(s"Overwrite bigQueryTable done", "bigQueryTable", bigQueryPartitionedTable, "hiveTable", hiveTable, "hivePartitionBTimes", hivePartitionBTimes)

  }


  @volatile private var lastLengthMap: FixedLinkedMap[String, java.lang.Long] = new FixedLinkedMap[String, java.lang.Long](24*3)

  def copyTable (destTableName:String, sourceTableName: String, hiveTable: String, bqPartitionDay: String, hivePartitionBDate: String): Unit ={

    LOG.warn("Copy table start", "destTableName", destTableName, "sourceTableName", sourceTableName, "hiveTable", hiveTable, "copy_bqDay",bqPartitionDay,"hivePartitionBDate",hivePartitionBDate)

    val destT = TableId.of(reportDataset, s"$destTableName")
    val sourceT = TableId.of(reportDataset, s"$sourceTableName")

    val configuration = CopyJobConfiguration
      .newBuilder(destT, sourceT)
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
      .build

    var job = bigquery.create(JobInfo.of(configuration))
    job = job.waitFor()
    LOG.warn("Copy table done", "destTableName", destTableName, "sourceTableName", sourceTableName, "hiveTable", hiveTable, "copy_bqDay",bqPartitionDay,"hivePartitionBDate",hivePartitionBDate, "status", job.getStatus)

  }

  def deleteTable(tableName: String): Unit= {
    LOG.warn("Delete table start", tableName);
    bigquery.delete(TableId.of(reportDataset, tableName))
    LOG.warn("Delete table done", tableName);
  }

  def deleteTables(byTableNameContains: String*): Unit ={
    val tables = bigquery.listTables(reportDataset, TableListOption.pageSize(Integer.MAX_VALUE+0L)).getValues.iterator()

    while(tables.hasNext) {
      var t = tables.next()
      val tn = t.getTableId.getTable
      var containsAll = true
      byTableNameContains.foreach{x=>
        if(!tn.contains(x)) {
          containsAll = false
        }
      }
      if(byTableNameContains.length == 0) {
        containsAll = false
      }

      if(containsAll) {
        LOG.warn(s"Delete table by contains", "byTableNameContains", byTableNameContains, "table", t.getTableId)
        t.delete()
      }
    }
  }

}

//object M{
//  def main (args: Array[String]): Unit = {
//    val bqDay="sss"
//   val  bigQueryTable = "444"
//    println(s"""$bigQueryTable$$$bqDay""")
//
//  }
//}