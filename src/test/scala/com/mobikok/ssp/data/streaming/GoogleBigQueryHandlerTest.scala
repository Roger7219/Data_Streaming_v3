package com.mobikok.ssp.data.streaming

import java.io.{FileInputStream, InputStream}
import java.nio.channels.Channels

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.HdfsConfiguration

/**
  * Created by admin on 2017/9/4.
  */
class GoogleBigQueryHandlerTest  {


  val bigquery: BigQuery = BigQueryOptions.newBuilder.setProjectId("dogwood-seeker-182806")
    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("key.json"))).build.getService
  val conf = new HdfsConfiguration()
  val fileSystem = FileSystem.get(conf)

  def uploadFile2Bigquery(table:String, in: InputStream, date: String): Unit ={

    println("Bigquery upload starting" + s"table: $table\ndate: $date")

    val  tableId = TableId.of("my_new_dataset", table)
    val writeChannelConfiguration = WriteChannelConfiguration
      .newBuilder(tableId)
      .setFormatOptions(FormatOptions.csv())
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .build()
    val writer = bigquery.writer(writeChannelConfiguration)

    val stream = Channels.newOutputStream(writer)
    IOUtils.copy(in, stream)
    stream.close()

    var job = writer.getJob()
    job = job.waitFor()
    val stats = job.getStatistics[JobStatistics]()
    println("Bigquery upload done !!" + s"table: $table\ndate: $date\nstats: $stats")
  }
}












//
//object  x{
//
//  def main (args: Array[String]): Unit = {
//    (1 to 50).par.foreach{
//      println(_)
//    }
//  }
//}

//
//
//
//val bigquery = BigQueryOptions.getDefaultInstance.getService
//val csvPath = Paths.get(new File("C:/Users/Administrator/Desktop/part-00000-501550c7-ea43-4058-ace3-0e8657bc0604.csv.gz").toURI)
//
//val  tableId = TableId.of("my_new_dataset", "upload_campagin_table")
//val writeChannelConfiguration =
//WriteChannelConfiguration.newBuilder(tableId)
//.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//.setAutodetect(true)
//.setSchema(
//Schema.newBuilder()
//.addField(Field.of("publisherid", Type.integer()))
//.addField(Field.of("appid", Type.integer()))
//.addField(Field.of("countryid", Type.integer()))
//.addField(Field.of("carrierid", Type.integer()))
//.addField(Field.of("adtype", Type.integer()))
//.addField(Field.of("campaignid", Type.integer()))
//.addField(Field.of("offerid", Type.integer()))
//.addField(Field.of("imageid", Type.integer()))
//.addField(Field.of("affsub", Type.string()))
//.addField(Field.of("requestcount", Type.integer()))
//.addField(Field.of("sendcount", Type.integer()))
//.addField(Field.of("showcount", Type.integer()))
//.addField(Field.of("clickcount", Type.integer()))
//.addField(Field.of("feereportcount", Type.integer()))
//.addField(Field.of("feesendcount", Type.integer()))
//.addField(Field.of("feereportprice", Type.floatingPoint()))
//.addField(Field.of("feesendprice", Type.floatingPoint()))
//.addField(Field.of("cpcbidprice", Type.floatingPoint()))
//.addField(Field.of("cpmbidprice", Type.floatingPoint()))
//.addField(Field.of("conversion", Type.integer()))
//.addField(Field.of("allconversion", Type.integer()))
//.addField(Field.of("revenue", Type.floatingPoint()))
//.addField(Field.of("realrevenue", Type.floatingPoint()))
//.addField(Field.of("l_time", Type.string()))
//.addField(Field.of("b_date", Type.string()))
//
//.addField(Field.of("publisheramid", Type.integer()))
//.addField(Field.of("publisheramname", Type.string()))
//.addField(Field.of("advertiseramid", Type.integer()))
//.addField(Field.of("advertiseramname", Type.string()))
//.addField(Field.of("appmodeid", Type.integer()))
//.addField(Field.of("appmodename", Type.string()))
//.addField(Field.of("adcategory1id", Type.integer()))
//.addField(Field.of("adcategory1name", Type.string()))
//.addField(Field.of("campaignname", Type.string()))
//.addField(Field.of("adverid", Type.integer()))
//.addField(Field.of("advername", Type.string()))
//.addField(Field.of("offeroptstatus", Type.integer()))
//.addField(Field.of("offername", Type.string()))
//.addField(Field.of("publishername", Type.string()))
//.addField(Field.of("appname", Type.string()))
//.addField(Field.of("countryname", Type.string()))
//.addField(Field.of("carriername", Type.string()))
//.addField(Field.of("adtypeid", Type.integer()))
//.addField(Field.of("adtypename", Type.string()))
//.addField(Field.of("versionid", Type.integer()))
//.addField(Field.of("versionname", Type.string()))
//.addField(Field.of("publisherproxyid", Type.integer()))
//
//.build()
//
//)
//.setFormatOptions(FormatOptions.csv())
//.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
//.build()
//val writer = bigquery.writer(writeChannelConfiguration)
//// Write data to writer
//try  {
//val stream = Channels.newOutputStream(writer)
//Files.copy(csvPath, stream)
//}catch {case e:Exception=>e.printStackTrace()}
//
//System.out.println()
//// Get load job
//var job = writer.getJob()
//job = job.waitFor()
//val stats = job.getStatistics()
//println(stats)
