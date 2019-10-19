package com.mobikok.ssp.data.streaming.handler.dwi

import java.util

import com.mobikok.ssp.data.streaming.OptimizedMixApp.{argsConfig, version}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.s
import com.mobikok.ssp.data.streaming.udf.UserAgentBrowserKernelUDF.StringUtil
import com.mobikok.ssp.data.streaming.util.{MC, OM, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

//object SQLHandler{
//  def main(args: Array[String]): Unit = {
//    println(
//      s"""
//         |drop view if exists nadx_p_matched_dwi_tmp;
//         |-- drop table xxx;
//         |        -- create temporary view nadx_p_matched_dwi_tmp as
//         |        select *
//         |        from;
//         |        select
//         |          -- sss;
//         |          id
//         |        from sss
//         |
//       """.stripMargin
//        .replaceAll("\\s*--[^\n]+","")
//      .split(";\\s*[\n]")
////      .map{x=>x.replaceAll("\\s*--[^\n]+","")}
//      .filter{x=>StringUtil.notEmpty(x)}
//
//        .mkString("================"))
//
////    var ps =List(HivePartitionPart("b","b_val"), HivePartitionPart("b", "b_val2"))
////    print(OM.toJOSN(ps.toSet[HivePartitionPart].map{x=>Array(x)}.toArray))
//
//  }
//}
class SQLHandler extends Handler {

  var plainSql: String = ""
  var sqlSegments: Array[String] = null

  var messageTopics: Array[String] = null
  var messageConsumer: String = null

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)

    messageTopics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))
    messageConsumer = versionFeaturesKafkaCer(version, handlerConfig.getString("message.consumer"))

    plainSql = handlerConfig.getString("sql")
    sqlSegments = plainSql
      .replaceAll("\\s*--[^\n]+","")
      .split(";\\s*[\n]")
      .filter{x=>StringUtil.notEmpty(x)}

    if(ArgsConfig.Value.OFFSET_LATEST.equals(argsConfig.get(ArgsConfig.OFFSET))){
      MC.setLastestOffset(messageConsumer, messageTopics)
    }
  }

  override def handle(newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"SQLHandler handle start")

    // newDwi 是空的
    var resultDF: DataFrame = newDwi
    RunAgainIfError.run{
      MC.pullBTimeDesc(messageConsumer, messageTopics, ps =>{

        if(ps.size > 0) {
          var w = hiveClient.partitionsWhereSQL(ps.toSet[HivePartitionPart].map{x=>Array(x)}.toArray)

          sqlSegments.foreach{x=>
            var s = x.replaceAll("""\$\{b_time_where\}""", w)
            // 只处理最近3小时的数据
            resultDF = sql(s)
          }
        }

        true;
      })
    }

    //只处理最近6小时的数据
//    if(resultDF != null) resultDF = resultDF.where("b_time >= date_format(current_timestamp() + INTERVAL -5 HOUR, 'yyyy-MM-dd HH:00:00')");

    LOG.warn(s"SQLHandler handle done")
    (resultDF, Array())
  }

//  def collectDWIBTimes(newDwi: DataFrame): Array[Array[HivePartitionPart]] = {
//    val ts = Array("l_time", "b_date", "b_time")
//    var result = newDwi
//      .dropDuplicates(ts)
//      .collect()
//      .map { x =>
//        ts.map { y =>
//          HivePartitionPart(y, x.getAs[String](y))
//        }
//      }
//
//    LOG.warn("collectNewDWIBTimes",  result)
//    result
//  }

  override def init (): Unit = {}

  override def commit (cookies: TransactionCookie): Unit = {
//    hiveClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
//    if(joinedDF != null) joinedDF.unpersist()

//    var result = Array[TransactionCookie]()
//
//    val mixTransactionManager = transactionManager.asInstanceOf[MixTransactionManager]
//    if (mixTransactionManager.needTransactionalAction()) {
//      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
//      result = needCleans.toArray
//      batchTransactionCookiesCache.removeAll(needCleans)
//    }
//    hiveClient.clean(result:_*)
  }

  private def versionFeaturesKafkaCer(version: String, kafkaCer: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) kafkaCer else s"${kafkaCer}_v${version}".trim
  }
  private def versionFeaturesKafkaTopic(version: String, kafkaTopic: String): String = {
    return if(ArgsConfig.Value.VERSION_DEFAULT.equals(version)) kafkaTopic else s"${kafkaTopic}_v${version}".trim
  }
}
