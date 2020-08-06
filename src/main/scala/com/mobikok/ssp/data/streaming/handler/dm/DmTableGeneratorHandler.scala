package com.mobikok.ssp.data.streaming.handler.dm

import java.{lang, util}

import com.mobikok.message.client.MessageClient
import com.mobikok.message.{Message, Resp}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{MessageClientUtil, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, SaveMode}

import scala.collection.JavaConversions._
/**
  * 用AccelerateTableHandler代替。
  * 加速表生成器，减少将原有含多个维度的数据并生成新的表(base表+目前l_time的原表数据)，实现加速查询。
  * Created by admin on 2017/9/4.
  */
@deprecated
class DmTableGeneratorHandler extends Handler {

  //view, consumer, topics, sql, is_inited_base_table, unionGroup, unionSum
  var viewConsumerTopics = null.asInstanceOf[Array[(String, String, Array[String], Array[String], Array[Boolean], Array[Column], Array[Column], String)]]


  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val v = c.getString("view")
      var sql = c.getString("sql").split("\n").map {y=>if(y.trim.startsWith("--")) "" else y.trim }.mkString("\n").split(";").filter(_.trim.length>0)
      val mc = c.getString("message.consumer")
      val mt = c.getStringList("message.topics").toArray(new Array[String](0))
      LOG.warn("union.group", c.getString("union.group"))
      val unionGroup = c.getString("union.group").split("\\|").map{x=> LOG.warn("unionGroup field", x.trim); expr(x.trim) }
      LOG.warn("union.sum", c.getString("union.sum"))
      val unionSum = c.getString("union.sum").split("\\|").map{x=> LOG.warn("unionSum field", x.trim); expr(x.trim)}
      val createBaseTableSql = c.getString("createBaseTableSql").replaceAll(";", "")
      (v, mc, mt, sql, Array(false), unionGroup, unionSum, createBaseTableSql)
    }.toArray
  }

  def needInitBaseTable (): Unit ={

  }

  override def handle (): Unit = {
    LOG.warn("DmTableGeneratorHandler handler starting")

      viewConsumerTopics.foreach{ case(view, consumer, topics, templateSqls, needInit, unionGroup, unionSum, createBaseTableSql)=>

//        val (view, consumer, topics, templateSqls, needInit, unionGroup, unionSum, createBaseTableSql) = x
        RunAgainIfError.run({

          sql("set spark.sql.shuffle.partitions = 1")
          sql("set spark.default.parallelism = 1")

          var baseTable = s"${view}_base"
          var templateSql = templateSqls.head
          var needInitTopic = s"${view}_needInitBaseTable"
          var needInitConsumer = s"${needInitTopic}_cer"
          // Init
          MessageClientUtil.pullAndCommit(messageClient, needInitConsumer, new MessageClientUtil.Callback[Resp[java.util.List[Message]]] {
            override def doCallback (resp: Resp[util.List[Message]]): lang.Boolean = {

              //有消息有说明需要重新初始化baseTable, 无视消息具体内容
              if(resp.getPageData.size() == 0) {
                return false
              }

              LOG.warn(s"Init base table '${baseTable}' start")
              MessageClientUtil.pullAndSortByLTimeDescHivePartitionParts(
                messageClient,
                consumer,
                new MessageClientUtil.Callback[util.List[HivePartitionPart]] {
                  override def doCallback (resp: util.List[HivePartitionPart]): lang.Boolean = {

                    if(resp.isEmpty || resp.tail.isEmpty) {
                      return false
                    }

                    var tmpBaseTable = s"${baseTable}_tmp"
                    var createTmpBaseTableSql = createBaseTableSql.replaceAll(baseTable, tmpBaseTable)

                    var currLt = resp.head.value

                    sql(s"drop table if exists $tmpBaseTable")
                    sql(createTmpBaseTableSql)

                    sql(s""" set l_time = l_time < "${currLt}" """)
                    sql(templateSql).write.mode(SaveMode.Overwrite).insertInto(tmpBaseTable)

                    LOG.warn(s"Init-Overwrite pluggable base table done", tmpBaseTable)

                    sql(s"drop view if exists $view")
                    sql(s"drop table if exists $baseTable")
                    sql(s"alter table $tmpBaseTable rename to $baseTable")

                    sql(s""" set l_time = l_time = "${currLt}" """)
                    sql(
                      s"""create view ${view} as
                         | select * from $baseTable
                         | union all
                         | $templateSql
                      """.stripMargin)
//                    needInit(0) = true

                    return true
                  }
                },
                topics:_*
              )

              LOG.warn(s"Init base table done", baseTable)
              return true
            }
          }, needInitTopic)
//          if(!needInit(0)) {//is need init base table
//          }

          // Incr
          // 注意用的是含'Tail'的pullAndSortByLTimeDescTailHivePartitionParts,获取的是上上一些l_time(s)
          MessageClientUtil.pullAndSortByLTimeDescTailHivePartitionParts(
            messageClient,
            consumer,
            new MessageClientUtil.Callback[util.List[HivePartitionPart]] {

              def doCallback(resp: util.List[HivePartitionPart]): java.lang.Boolean = {

                if(resp.isEmpty) {
                  return true
                }

                LOG.warn(s"Incr table  for final dm start", "baseTable", baseTable, "dm", view, "incr l_time(s)", resp)

                sql(s"drop view if exists $view")

                sql(s""" set l_time = l_time in (${resp.map(_.value).mkString("'", ", ", "'")}) """)
                val base = hiveContext.read.table(baseTable)
                val fs = base.schema.fieldNames

                sql(templateSql)
                  .union(base)
                  .groupBy(unionGroup:_*)
                  .agg(
                    unionSum.head,
                    unionSum.tail:_*
                  )
                  .select(fs.head, fs.tail:_*)
                  .write
                  .mode(SaveMode.Overwrite)
                  .insertInto(baseTable)

                LOG.warn(s"Incr-Overwrite base table '${baseTable}' done")

                var prevLt = resp.head.value

                sql(s""" set l_time = l_time > "${prevLt}" """)

                sql(
                  s"""create view ${view} as
                     | select * from $baseTable
                     | union all
                     | $templateSql
                  """.stripMargin)
                LOG.warn("Incr table  for final dm done")
                return true
              }
            },
            topics:_*
          )
        })
      }

    LOG.warn("DmTableGeneratorHandler handler done")
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }
}

//
//object x{
//  def main (args: Array[String]): Unit = {
//    val vv= """
//      |date_format(b_date, 'yyyy-MM-01')|
//      |            publisherid-
//      |
//    """.stripMargin
//    println(vv.split("""\|""")(0))
//  }
//}