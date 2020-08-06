package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util.{MC, RegexUtil, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * 加速表生成器，减少将原有含多个维度的数据并生成新的表(base表+目前l_time的原表数据)，实现加速查询。
  * Created by admin on 2017/9/4.
  */
class AccelerateTableHandler extends Handler {
  //
  //  agg_traffic_month_dwr
  //  message.consumer = "agg_traffic_month_dwr_cer"
  //  message.topics = ["agg_traffic_dwr", "agg_traffic_month_dwr_re_init"]
  //
  //  createBaseTableSql= """
  //            create table if not exists agg_traffic_month_dwr_base(
  //                jarId     INT,
  //                countryId INT,
  //                showCount BIGINT,
  //                cost      DOUBLE
  //            )
  //            PARTITIONED BY (b_date STRING)
  //            STORED AS ORC;
  //        """
  //
  //  union.group="""
  //            date_format(b_date, 'yyyy-MM-01')|
  //            jarId|
  //            countryId
  //        """
  //  union.sum="""
  //            sum(showCount) as showCount|
  //            sum(cost)      as cost|
  //            date_format(b_date, 'yyyy-MM-01') as b_date
  //        """

  //view, consumer, topics, sql, is_inited_base_table, unionGroup, unionSum
  var viewConsumerTopics:Array[(String, String, Array[String], Array[String], Array[Column], Array[Column], String)] = null


  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    viewConsumerTopics = handlerConfig.getObjectList("items").map { x =>
      val c = x.toConfig
      val view = c.getString("view")  // "agg_traffic_month_dwr"
    var templateSqls = c.getString("sql").split("\n").map {y=>if(y.trim.startsWith("--")) "" else y.trim }.mkString("\n").split(";").map(_.trim).filter(_.length>0)

      var FIELD_SEPARATOR = "\000"
      var incrViewSql = templateSqls.last
      val sourceTable = RegexUtil.matchedGroups(incrViewSql.replaceAll("\\s*,\n", FIELD_SEPARATOR), "(?i)from\\s+([\\S]*)").head     //agg_traffic_dwr
    val consumer = s"${view}_cer"
      val topics = Array(sourceTable, s"${view}_re_init")
      LOG.warn("AccelerateTableHandler sourceTable", sourceTable)

      val unionGroup = RegexUtil.matchedGroups(incrViewSql.replaceAll("\\s*,\n", FIELD_SEPARATOR), "(?i)group by\\s*(.*?)\\s?$", s"\\s*([^$FIELD_SEPARATOR]+)").map(expr(_)).toArray
      LOG.warn("AccelerateTableHandler unionGroup", unionGroup)

      val unionSelect = RegexUtil.matchedGroups(incrViewSql.replaceAll("\\s*,\n", FIELD_SEPARATOR), "(?i)select\\s*(.*?)\\s+from", s"\\s*([^$FIELD_SEPARATOR]+)" ).map(expr(_)).toArray
      LOG.warn("AccelerateTableHandler unionSelect", unionSelect)

      val fieldNames = RegexUtil.matchedGroups(incrViewSql.replaceAll("\\s*,\n", FIELD_SEPARATOR), "(?i)select\\s*(.*?)\\s+from", s"\\s*([^$FIELD_SEPARATOR]+)", "\\s*(\\S+)$").toArray
      LOG.warn("AccelerateTableHandler fieldNames", fieldNames)

      //分区字段
      var pf = ListBuffer[String]()
      //普通字段

      var sourceFS = hiveContext.table(sourceTable).schema.fields
      val fs = fieldNames
        .map{x=>
          val fw = sourceFS.filter{y=>y.name.toLowerCase.equals(x.toLowerCase)}
          val f = if(fw.length > 0) fw.head else null

          if(f == null) {
            throw new HandlerException(s"Accelerate table field name '$x' is not in the original table '$sourceTable'.")
          }else if("b_date".equals(f.name) || "l_time".equals(f.name) || "b_time".equals(f.name)) {
            pf.append(s"${f.name} STRING")
            null
          }
          else {
            f
          }
        }
        .filter(_ != null)
        .map(x=>s"`${x.name}` ${x.dataType.simpleString}")
        .mkString(",\n")

//      var fs = hiveContext.table(sourceTable).schema
//        .fields
//        .filter{x=>
//          val n = x.name
//          val ln = n.toLowerCase
//          if(fieldNames.map(_.toLowerCase).contains(ln)) {
//            if("l_time".equals(ln) || "b_date".equals(ln) || "b_time".equals(ln)) {
//              pf.append(s"$n STRING")
//              false
//            }
//            else true
//          }else {
//            throw new HandlerException(s"Accelerate table field name '$n' is not in the original table '$sourceTable'.")
//          }
//        }
//        .map(x=>s"`${x.name}` ${x.dataType.simpleString}")
//        .mkString(",\n")

      var pfs = ""

      if(!pf.contains("l_time")) {
        throw new HandlerException("Accelerate table must contain the field 'l_time', But without it.")
      }

      if(pf.size>0) {
        pfs = pf.mkString("PARTITIONED BY (", ", ", ")")
      }

      val createBaseTableSql = s"""create table if not exists ${view}_base(
         | $fs
         |)$pfs
         |STORED AS ORC
       """.stripMargin

      LOG.warn("AccelerateTableHandler createBaseTableSql", createBaseTableSql)

      (view, consumer, topics, templateSqls,  unionGroup, unionSelect, createBaseTableSql)
    }.toArray
  }

  def needInitBaseTable (): Unit ={

  }

  override def handle (): Unit = {
    LOG.warn("AccelerateTableHandler handler starting")

    viewConsumerTopics.foreach{ case(view, consumer, topics, templateSqls, unionGroup, unionSelect, createBaseTableSql)=>

      //        val (view, consumer, topics, templateSqls, needInit, unionGroup, unionSelect, createBaseTableSql) = x
      RunAgainIfError.run({

        sql("set spark.sql.shuffle.partitions = 1")
        sql("set spark.default.parallelism = 1")

        var baseTable = s"${view}_base"
        var incrViewSql = templateSqls.last
        var prevSqls = templateSqls.reverse.tail.reverse
        var needInitTopic = s"${view}_needInitBaseTable"
        var needInitConsumer = s"${needInitTopic}_cer"
        // Init
        MC.pull(needInitConsumer, Array(needInitTopic), {resp=>

          //有消息有说明需要重新初始化baseTable, 无视消息具体内容
          if(resp.size() > 0) {
            LOG.warn(s"Init base table '${baseTable}' start")
            MC.pullLTimeDesc(
              consumer,
              topics,
              {l_times=>
                //至少包含两个l_time,一个是当前的，另一些此前的
                if(l_times.isEmpty || l_times.tail.isEmpty) {
                  false
                }else {
                  var tmpBaseTable = s"${baseTable}_tmp"
                  var createTmpBaseTableSql = createBaseTableSql.replaceAll(baseTable, tmpBaseTable)

                  var currLt = l_times.head.value

                  sql(s"drop table if exists $tmpBaseTable")
                  sql(createTmpBaseTableSql)

                  sql(s""" set l_time = l_time < "${currLt}" """)

                  //
                  prevSqls.foreach(x=> sql(x))

                  sql(
                    s"""
                       |insert overwrite table $tmpBaseTable
                       |${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time < "${currLt}" """)}
                     """.stripMargin)

//                  sql(incrViewSql).write.mode(SaveMode.Overwrite).insertInto(tmpBaseTable)

                  LOG.warn(s"Init-Overwrite pluggable base table done", tmpBaseTable)

                  sql(s"drop view if exists $view")
                  sql(s"drop table if exists $baseTable")
                  sql(s"alter table $tmpBaseTable rename to $baseTable")

                  sql(s""" set l_time = l_time = "${currLt}" """)
                  sql(
                    s"""create view ${view} as
                       | select * from $baseTable
                       | union all
                       | ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time = "${currLt}" """)}
                      """.stripMargin)
                  //                    needInit(0) = true
                  true
                }
              }
            )
            LOG.warn(s"Init base table done", baseTable)
          }
          true
        })
        //          if(!needInit(0)) {//is need init base table
        //          }

        // 待添加自检
        // Incr
        // 注意用的是含'Tail'的pullAndSortByLTimeDescTailHivePartitionParts,获取的是上上一些l_time(s)
        MC.pullLTimeDescTail(
          consumer,
          topics,
          {l_times =>
            if(l_times.nonEmpty) {
              LOG.warn(s"Incr table  for final dm start", "baseTable", baseTable, "dm", view, "incr l_time(s)", l_times)

              sql(s"drop view if exists $view")

              sql(s""" set l_time = l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """)

              //
              prevSqls.foreach(x=> sql(x))

              val base = hiveContext.read.table(baseTable)
              val fs = base.schema.fieldNames

              sql(
                s"""
                   |insert overwrite table $baseTable
                   |select ${fs.mkString(", ")}
                   |from (
                   |  ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """)}
                   |  union all
                   |  select * from $baseTable
                   |  where l_time in (${l_times.map(_.value).mkString("'", "', '", "'")})
                   |)t
                   |group by ${unionGroup.mkString(", ")}
                   |
                 """.stripMargin)

//              sql(incrViewSql)
//                .union(base.where(s" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) "))
//                .groupBy(unionGroup:_*)
//                .agg(
//                  unionSelect.head,
//                  unionSelect.tail:_*
//                )
//                .select(fs.head, fs.tail:_*)
//                .write
//                .mode(SaveMode.Overwrite)
//                .insertInto(baseTable)

              LOG.warn(s"Incr-Overwrite base table '${baseTable}' done")

              var prevLt = l_times.head.value

              sql(s""" set l_time = l_time > "${prevLt}" """)

              sql(
                s"""create view ${view} as
                   | select * from $baseTable
                   | union all
                   | ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time > "${prevLt}" """)}
                  """.stripMargin)
              LOG.warn("Incr table  for final dm done")
            }
            true
          }
        )

      })
    }

    LOG.warn("AccelerateTableHandler handler done")
  }

  //  def sql(sqlText: String): DataFrame ={
  //    LOG.warn("Execute HQL", sqlText)
  //    hiveContext.sql(sqlText)
  //  }
}

object AccelerateTableHandlerTest{
  def main (args: Array[String]): Unit = {
    val incrViewSql = "create table ${l_time} asd\n12 ${l_time}"
    System.out.println(incrViewSql.replaceAll("""\$\{l_time\}""", "33333"))
  }
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