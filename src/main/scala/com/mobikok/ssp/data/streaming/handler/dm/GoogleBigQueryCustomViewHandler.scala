package com.mobikok.ssp.data.streaming.handler.dm

import java.util

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{JavaMessageClient, MessageClient, ModuleTracer, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * 上传自定义视图数据到BigQuery
  * Created by admin on 2017/9/4.
  */
class GoogleBigQueryCustomViewHandler extends Handler {

  //view, consumer, topics, sql, b_date
  var viewConsumerTopics = null.asInstanceOf[Array[(String, String, Array[String], Array[String], String)]]

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val v = c.getString("view")
      var sql = c.getString("sql").split("\n").map {y=>if(y.trim.startsWith("--")) "" else y.trim }.mkString("\n").split(";").filter(_.trim.length>0)
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      var bDateExpr = c.getString("set.b_date")
      (v, mc, mt, sql, bDateExpr)
    }.toArray
  }

  override def doHandle (): Unit = {
    LOG.warn("GoogleBigQueryHandler handler starting")

    viewConsumerTopics.foreach{ x=>

      JavaMessageClient.pullAndSortByBDateDescHivePartitionParts(
        messageClient.messageClientApi,
        x._2,
        new JavaMessageClient.Callback[util.List[HivePartitionPart]] {

          def doCallback(resp: util.List[HivePartitionPart]) :java.lang.Boolean = {

            resp.foreach{y=>

              RunAgainIfError.run({
//                val bdExpr = x._5.replaceAll("\\$\\{b_date\\}", y.getValue.split(" ")(0))
                val bdExpr = x._5.replaceAll("\\$\\{b_date\\}", y.getValue)  // xxx
                val bd = sql(s"SELECT $bdExpr").take(1)(0).getString(0)
                sql(s""" set b_date = "${bd}" """)

                x._4.foreach{z=>
                  sql(z)
                }

                bigQueryClient.overwrite/*ByBDate*/(x._1, x._1, bd)

              }, "GoogleBigQueryHandlerV3 handler fail")

            }
            return true
          }
        },
        x._3:_*
      )
    }

    LOG.warn("GoogleBigQueryHandler handler done")
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }
}