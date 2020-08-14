package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.transaction.TransactionManager
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class SQLHandler extends Handler {

  var plainSql: String = ""
  var sqlSegments: Array[String] = null

  var messageTopics: Array[String] = null
  var messageConsumer: String = null

  var inputDwiAsViewName = "dwi"
  var messageClientPullDataAsViewName = "ps"  // ps: partitions简称

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)

    messageTopics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))
    messageConsumer = versionFeaturesKafkaCer(version, handlerConfig.getString("message.consumer"))

    inputDwiAsViewName = if(handlerConfig.hasPath("dwi.view")) handlerConfig.getString("dwi.view") else inputDwiAsViewName
    messageClientPullDataAsViewName = if(handlerConfig.hasPath("message.view")) handlerConfig.getString("message.view") else messageClientPullDataAsViewName

    plainSql = handlerConfig.getString("sql")
    sqlSegments = plainSql
      .replaceAll("\\s*--[^\n]+","")
      .split(";\\s*[\n]")
      .filter{x=>StringUtil.notEmpty(x)}

    if(ArgsConfig.Value.OFFSET_LATEST.equals(argsConfig.get(ArgsConfig.OFFSET))){
      messageClient.setLastestOffset(messageConsumer, messageTopics)
    }
  }

  override def doHandle(newDwi: DataFrame): DataFrame = {

    LOG.warn(s"SQLHandler handle start")
    var resultDF: DataFrame = null
    RunAgainIfError.run{

      resultDF = newDwi

      messageClient.pullBTimeDesc(messageConsumer, messageTopics, bTimes =>{

        newDwi.createOrReplaceTempView(inputDwiAsViewName)

        val partitions= bTimes.toSet[HivePartitionPart].map{x=>Array(x)}.toArray
        hiveClient.partitionsAsDataFrame(partitions).createOrReplaceTempView(messageClientPullDataAsViewName)

        sqlSegments.foreach{s=>
          resultDF = sql(s)
        }

        //提交偏移
        true
      })
    }

    LOG.warn(s"SQLHandler handle done")
    resultDF
  }

  override def doCommit (): Unit = {}

  override def doClean (): Unit = {}

  private def versionFeaturesKafkaCer(version: String, kafkaCer: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) kafkaCer else s"${kafkaCer}_v${version}".trim
  }

  @Deprecated
  private def versionFeaturesKafkaTopic(version: String, kafkaTopic: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) kafkaTopic else s"${kafkaTopic}_v${version}".trim
  }
}
