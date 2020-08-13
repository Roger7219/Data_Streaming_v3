package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{MC, ModuleTracer, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * 上传自定义视图数据到BigQuery
  * Created by admin on 2017/9/4.
  */
class GoogleBigQueryCustomViewOfflineHandler extends Handler {

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
    LOG.warn("GoogleBigQueryCustomViewOfflineHandler handler start")

    viewConsumerTopics.foreach{x=>

      MC.pullBDateDescTail(x._2, x._3, {resp=>
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

          }, "GoogleBigQueryCustomViewOfflineHandler handler fail")

        }
        true
      })
    }

    LOG.warn("GoogleBigQueryCustomViewOfflineHandler handler done")
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }
}

//object GoogleBigQueryCustomViewOfflineHandlerTest{
//  def main(args: Array[String]): Unit = {
//    MC.init(new MessageClient("http://node14:5555"))
//
//    //test scala
//    MC.pullBDateDescTail("ssp_topn_dm_cer", Array("ssp_report_overall_dwr_day"), {resp=> // scala版的回调函数
//      println("eeee:"+resp)
//
//      true //正确，直接写返回值
//      // return true 错误，scala版的回调函数，返回值不能加return, 加了不会执行后面的代码，如：println("the end")，会直接跳出main方法
//    })
//
//    // test java
////    MessageClientUtil.pullAndSortByBDateDescTailHivePartitionParts(new MessageClient("http://node14:5555"), "ssp_topn_dm_cer4", new MessageClientUtil.Callback[util.ArrayList[HivePartitionPart]] {
////      override def doCallback (resp: util.ArrayList[HivePartitionPart]): lang.Boolean = { // java版的回调函数
////        println("eeee:"+resp)
////
////        return true //没错误，java版的回调函数不会出现sacla的问题。
////      }
////    }, Array("ssp_report_overall_dwr_day"):_*)
//
//    println("the end")
//  }
//}