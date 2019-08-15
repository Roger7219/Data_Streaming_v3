package com.mobikok.ssp.data.streaming.handler.dm.offline

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{OM, RunAgainIfError, StringUtil}
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

/**
  * 用于将hour表转成day/month/year表，或者day表转成month/year表
  * 不修改信息直接用sql，速度较快
  * 直接通过sql在group by的时候会保持l_time, b_date, b_time的信息
  */
class HistoryDataHandler extends Handler {

  //view, consumer, topics
  var viewConsumerTopics: Array[(String, String, Array[String])] = null.asInstanceOf[Array[(String, String, Array[String])]]
  var groupByFields: Array[(String, String)] = _
  var aggFields: Array[String] = _
  var unionFields: Array[(String, String)] = _

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext,argsConfig, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val v = c.getString("view")
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      (v, mc, mt)
    }.toArray

//    groupByFields = handlerConfig.getConfigList("groupby.fields")
//      .map{ field => s"""${field.getString("expr")} as ${field.getString("as")}"""}
//      .toArray ++ Array("l_time", "b_date", "b_time")
    groupByFields = handlerConfig
      .getConfigList("groupby.fields")
      .map{ field => (field.getString("expr"), field.getString("as"))}
      .toArray ++ Array (
        ("date_format(l_time, 'yyyy-MM-dd 00:00:00')", "l_time"),
        ("date_format(b_date, 'yyyy-MM-dd')", "b_date"),
        ("date_format(b_time, 'yyyy-MM-dd 00:00:00')", "b_time")
      )

    aggFields = handlerConfig.getConfigList("groupby.aggs").map{field => s"""${field.getString("union")} as ${field.getString("as")}"""}.toArray
    unionFields = handlerConfig.getConfigList("groupby.aggs").map{field => (field.getString("expr"), field.getString("as"))}.toArray
  }

  override def handle(): Unit = {
    LOG.warn("HistoryDataHandler handler starting")
    RunAgainIfError.run{
      viewConsumerTopics.foreach{ topics =>
        val pageData = messageClient
          .pullMessage(new MessagePullReq(topics._2, topics._3))
          .getPageData

        val ms = pageData.map{ x=>
          OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
        }.flatMap{x=>x}
          .flatMap{x=>x}
          //        .filter{x=>"b_time".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  }
          .filter{x=>"b_time".equals(x.name) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  } //xxx
          .distinct
          .sortBy(_.value)(Ordering.String.reverse)
          .toArray
        LOG.warn(s"HistoryDataHandler update b_time(s), count: ${ms.length}", ms)
        // TODO import here

        val schema = hiveContext.read.table("test__ssp_report_overall_dwr_accday").schema.fieldNames
        val aggColumns = aggFields.map{x => expr(x)}
        ms.par.foreach{ b_time =>
          // day
//          hiveContext
//            .read
//            .table("ssp_report_overall_dwr")
//            .selectExpr("publisherid",
//              "appid",
//              "countryid",
//              "carrierid",
//              "versionname",
//              "adtype",
//              "campaignid",
//              "offerid",
//              "imageid",
//              "affsub",
//              "requestcount",
//              "sendcount",
//              "showcount",
//              "clickcount",
//              "feereportcount",
//              "feesendcount",
//              "feereportprice",
//              "feesendprice",
//              "cpcbidprice",
//              "cpmbidprice",
//              "conversion",
//              "allconversion",
//              "revenue",
//              "realrevenue",
//              "feecpctimes",
//              "feecpmtimes",
//              "feecpatimes",
//              "feecpasendtimes",
//              "feecpcreportprice",
//              "feecpmreportprice",
//              "feecpareportprice",
//              "feecpcsendprice",
//              "feecpmsendprice",
//              "feecpasendprice",
//              "null as packagename",
//              "null as domain",
//              "null as operatingsystem",
//              "null as systemlanguage",
//              "null as devicebrand",
//              "null as devicetype",
//              "null as browserkernel",
//              "0 as respstatus",
//              "0 as winprice",
//              "0 as winnotices",
//              "0 as test",
//              "0 as ruleid",
//              "0 as smartid",
//              "newcount",
//              "activecount",
//              "null as eventname",
//              "0 as recommender",
//              "date_format(l_time, 'yyyy-MM-dd 00:00:00') as l_time",
//              "date_format(b_date, 'yyyy-MM-dd') as b_date",
//              "date_format(b_time, 'yyyy-MM-dd 00:00:00') as b_time"
//            )
//            .where(s"""b_date='${b_time.value.split(" ")(0)}'""")
//            .groupBy(groupByFields.map{x => expr(x._2)}:_*)
//            .agg(aggColumns.head, aggColumns.tail:_*)
//            .coalesce(1)
//            .select(schema.head, schema.tail:_*) // group by 和 agg会打乱原本的schema的顺序，需要重新select恢复成原来的顺序再插入
//            .write
//            .format("orc")
//            .mode(SaveMode.Overwrite)
//            .insertInto(s"test__ssp_report_overall_dwr_accday")
          // month
          val year = b_time.value.split(" ")(0).split("-")(0)
          val month = b_time.value.split(" ")(0).split("-")(1)
            hiveContext
            .read
            .table(topics._1)
            .selectExpr("publisherid",
              "appid",
              "countryid",
              "carrierid",
              "versionname",
              "adtype",
              "campaignid",
              "offerid",
              "imageid",
              "affsub",
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
              "null as packagename",
              "null as domain",
              "null as operatingsystem",
              "null as systemlanguage",
              "null as devicebrand",
              "null as devicetype",
              "null as browserkernel",
              "0 as respstatus",
              "0 as winprice",
              "0 as winnotices",
              "0 as test",
              "0 as ruleid",
              "0 as smartid",
              "newcount",
              "activecount",
              "null as eventname",
              "0 as recommender",
              "date_format(l_time, 'yyyy-MM-01 00:00:00') as l_time",
              "date_format(b_date, 'yyyy-MM-01') as b_date",
              "date_format(b_time, 'yyyy-MM-01 00:00:00') as b_time"
            )
            .where(s"""year(b_time) = $year and month(b_time) = $month""")
            .groupBy(groupByFields.map{x => expr(x._2)}:_*)
            .agg(aggColumns.head, aggColumns.tail:_*)
            .coalesce(1)
            .select(schema.head, schema.tail:_*) // group by 和 agg会打乱原本的schema的顺序，需要重新select恢复成原来的顺序再插入
            .write
            .format("orc")
            .mode(SaveMode.Overwrite)
            .insertInto(s"test__ssp_report_overall_dwr_accmonth")
          LOG.warn(s"Finish insert into test__ssp_report_overall_dwr_accmonth")
        }

        messageClient.commitMessageConsumer(
          pageData.map { d=>
            new MessageConsumerCommitReq(topics._2, d.getTopic, d.getOffset)
          }:_*
        )
      }
    }
  }
}
