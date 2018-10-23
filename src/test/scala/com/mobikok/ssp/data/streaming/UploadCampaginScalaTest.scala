package com.mobikok.ssp.data.streaming

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.Channels

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration

/**
  * Created by Administrator on 2017/10/19.
  */
object UploadCampaginScalaTest {
  System.setProperty("http.proxySet", "true");    //设置使用网络代理
  System.setProperty("http.proxyHost", "192.168.1.6");  //设置代理服务器地址
  System.setProperty("http.proxyPort", "808");    //设置代理服务器端口号

  System.setProperty("proxySet", "true");
  System.setProperty("socksProxyHost", "192.168.1.6");
  System.setProperty("socksProxyPort", "1080");

  val bigquery: BigQuery = BigQueryOptions.newBuilder
    .setProjectId("dogwood-seeker-182806")
    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("C:/Users/Administrator/Desktop/key.json"))).build.getService
  val conf = new HdfsConfiguration()
  val fileSystem = FileSystem.get(conf)

  def main (args: Array[String]): Unit = {

    val in = new  FileInputStream(new File("C:/Users/Administrator/Desktop/p2.gz"))
    uploadFile2Bigquery("upload_campagin_table", in, "20131212" )

  }

  def uploadFile2Bigquery(table:String, in: InputStream, date: String): Unit ={

    println("Bigquery upload starting" + s"table: $table\ndate: $date")

    //    val csvPath = Paths.get(gzFile.toURI)

    val  tableId = TableId.of("my_new_dataset", table)
    val writeChannelConfiguration = WriteChannelConfiguration
      .newBuilder(tableId)
      .setFormatOptions(FormatOptions.csv())
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .build()
    val writer = bigquery.writer(writeChannelConfiguration)
    // Write data to writer
    //    try  {
    val stream = Channels.newOutputStream(writer)
    //    Files.copy(csvPath, stream)
    IOUtils.copy(in, stream)
    stream.close()
    //    }catch {case e:Exception=> throw e}

    // Get load job
    var job = writer.getJob()
    job = job.waitFor()
    val stats = job.getStatistics[JobStatistics]()
    println("Bigquery upload done !!" + s"table: $table\ndate: $date\nstats: ${stats}")


  }

}
