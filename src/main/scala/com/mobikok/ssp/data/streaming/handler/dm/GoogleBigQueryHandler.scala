package com.mobikok.ssp.data.streaming.handler.dm

import java.io.InputStream
import java.nio.channels.Channels

import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery._
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.OM
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * 待删，用GoogleBigQueryHandlerV2代替
  * Created by admin on 2017/9/4.
  */
@Deprecated
class GoogleBigQueryHandler extends Handler {

  //view, consumer, topics
  var viewConsumerTopics = null.asInstanceOf[Array[(String, String, Array[String])]]

  override def init (moduleName: String, bigQueryClient:BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient ,greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val v = c.getString("view")
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      (v, mc, mt)
    }.toArray
  }

  override def handle (): Unit = {
    LOG.warn("GoogleBigQueryHandler handler starting")

    viewConsumerTopics.par.foreach{ x=>

      val pd = messageClient
        .pullMessage(new MessagePullReq(x._2, x._3))
        .getPageData

      val ms = pd.map{x=>
          OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
        }
        .flatMap{x=>x}
        .flatMap{x=>x}
        .filter{x=>"b_date".equals(x.name)}
        .distinct
        .toArray

      ms.foreach{y=>
        var gzDir = s"/root/kairenlo/pluggable/${x._1}_${y.getValue.replaceAll("-","_")}_gz.dir"
        LOG.warn("GoogleBigQueryHandler generate gz file starting", gzDir)
        var bqDate = y.getValue.replaceAll("-", "")
        hiveContext
          .read
          .table(x._1)
          .where(s""" b_date = "${y.getValue}" """)
          .repartition(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("csv")
          .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
          .save(gzDir)

        LOG.warn("GoogleBigQueryHandler generate gz file completed", gzDir)

        //Must Only one file

        val gz = fileSystem
          .listStatus(new Path(gzDir), new PathFilter {
            override def accept (path: Path): Boolean = {
              path.getName.startsWith("part-")
            }
          })

        if(gz.size > 1) {
          throw new RuntimeException(s"Bigquery must upload only one file, but now it finds ${gz.size} in '${gzDir}' !!")
        }
        gz.foreach{f=>

            uploadFile2Bigquery("upload_campagin_table", fileSystem.open(f.getPath), bqDate)
          }
      }

      messageClient.commitMessageConsumer(
        pd.map {d=>
          new MessageConsumerCommitReq(x._2, d.getTopic, d.getOffset)
        }:_*
      )

    }

    LOG.warn("GoogleBigQueryHandler handler done")
  }

  import java.io.FileInputStream

  import com.google.auth.oauth2.ServiceAccountCredentials
  import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}

  val bigquery: BigQuery = BigQueryOptions.newBuilder.setProjectId("dogwood-seeker-182806").setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("key.json"))).build.getService
  val conf = new HdfsConfiguration()
  val fileSystem = FileSystem.get(conf)

//  val bigquery = BigQueryOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream("/root/kairenlo/data-streaming/key.json")).build().getService
    //.getDefaultInstance.getService

  def uploadFile2Bigquery(table:String, in: InputStream, date: String): Unit ={

    LOG.warn("Bigquery upload starting", s"table: $table\ndate: $date")

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
    LOG.warn("Bigquery upload done !!", s"table: $table\ndate: $date\nstats: $stats")
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
