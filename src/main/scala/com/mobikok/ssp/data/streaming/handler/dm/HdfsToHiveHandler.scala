package com.mobikok.ssp.data.streaming.handler.dm

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClientApi
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * 从导入的ClickHouse数据中写在hdfs的json.gz的数据重新导入到hive中
  * 用于填回hive中缺失的小时数据，目前缺失了2018-07-19,2018-07-20，2018-07-21 3天的小时数据
  */
class HdfsToHiveHandler extends Handler {

  private var consumerTopics: (String, Array[String]) = _

  private val conf = new HdfsConfiguration()
  private val fileSystem = FileSystem.get(conf)

  private var fieldNames: Array[String] = _

  override def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    val consumer = handlerConfig.getString("message.consumer")
    val topics = handlerConfig.getStringList("message.topics").asScala.toArray
    consumerTopics = (consumer, topics)


    fieldNames = hiveContext.read.table("ssp_report_overall_dwr").schema.fieldNames
  }

  override def doHandle(): Unit = {

    RunAgainIfError.run {

      val pageData = messageClient.messageClientApi
        .pullMessage(new MessagePullReq(consumerTopics._1, consumerTopics._2))
        .getPageData

      val ms = pageData.map { data =>
        OM.toBean(data.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]] {})
      }.flatMap { data => data }
        .flatMap { data => data }
        .filter { partition => "b_time".equals(partition.name) && !"__HIVE_DEFAULT_PARTITION__".equals(partition.value) && StringUtil.notEmpty(partition.value) }
        .distinct
        .sortBy(_.value)(Ordering.String.reverse)
        .toArray

      ms.foreach { b_time =>

        LOG.warn(s"start run b_time: ${b_time.value}")
        val path = s"/root/kairenlo/data-streaming/test/ssp_report_overall_dm_${b_time.value.replace("-", "_").replace(":", "_").replace(" ", "__")}_gz.dir/"
        val gzFiles = fileSystem.listStatus(new Path(path), new PathFilter {
          override def accept(path: Path): Boolean = {
            path.getName.startsWith("part-")
          }
        })
        if (gzFiles.size != 1) {
          throw new HandlerException("GZFile must be one!")
        }
        val name = gzFiles(0).getPath.getName

        LOG.warn(s"gz file path: $path$name")

        val df = hiveContext.read.json(s"$path$name")

        val selectFields = fieldNames.map{ field =>
          if (df.schema.fieldNames.contains(field.toLowerCase())) {
            field
          } else {
            s"null as $field"
          }
        }

        df.selectExpr(selectFields: _*)
          .coalesce(1)
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto("ssp_report_overall_dwr")

        LOG.warn(s"restore b_time: $b_time data finished")
      }

      messageClient.messageClientApi.commitMessageConsumer(
        pageData.map{data =>
          new MessageConsumerCommitReq(consumerTopics._1, data.getTopic, data.getOffset)
        }:_*
      )
    }

  }
}
