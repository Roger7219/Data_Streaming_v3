package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, SaveMode}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * 加速表生成器，减少将原有含多个维度的数据并生成新的表(base表+目前l_time的原表数据)，实现加速查询。
  * Created by admin on 2017/9/4.
  */
class AccelerateTableHandlerV2 extends Handler {
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


  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

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
            pf.append(f.name)
            null
          }
          else {
            f
          }
        }
        .filter(_ != null)
        .map(x=>s"`${x.name}` ${x.dataType.simpleString}")
        .mkString(",\n")

      var pfs = ""

      if(!pf.contains("l_time")) {
        throw new HandlerException(s"Accelerate table '$view' must contain the partition field 'l_time', But without it.")
      }

      if(pf.size>0) {
        pfs = pf.mkString("PARTITIONED BY (", " STRING, ", " STRING)")
      }

      val createBaseTableSql = s"""create table if not exists ${view}_base(
         | $fs
         |)$pfs
         |STORED AS ORC
       """.stripMargin

      LOG.warn("AccelerateTableHandler createBaseTableSql", createBaseTableSql)

      // 首次建表初始化
      var baseTable = s"${view}_base"
      var lastLTimeTopic = s"${view}_lastLTimeTopic"
      var prevSqls = templateSqls.reverse.tail.reverse
      if(sql(s"show tables like '$baseTable'").count() == 0 /*|| sql(s"show tables like '$view'").count() == 0*/) {
        LOG.warn(s"Create-Init base table start", baseTable)
        initAccelerTable(lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
        LOG.warn(s"Create-Init base table done", baseTable)
      }

      (view, consumer, topics, templateSqls,  unionGroup, unionSelect, createBaseTableSql)
    }.toArray
  }

  def initAccelerTable(lastLTimeTopic:String, lastLTime: String, consumer: String, topics: Array[String], prevSqls: Array[String], baseTable: String, createBaseTableSql: String, incrViewSql: String, view: String): Unit = {
    MC.pullLTimeDesc(
      consumer,
      topics,
      {l_times=>
        if(l_times.isEmpty /*|| l_times.tail.isEmpty*/) {
          false
        }else {

          var tmpBaseTable = s"${baseTable}_tmp"
          var createTmpBaseTableSql = createBaseTableSql.replaceAll(baseTable, tmpBaseTable)

          var currLt: String = null
          if(lastLTime == null) {
            currLt = l_times.head.value
          }else {
            //降序
            currLt = (l_times.map(_.value) :+ lastLTime).sortBy{x=>x}.reverse.head
          }

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

//          sql(incrViewSql).write.mode(SaveMode.Overwrite).insertInto(tmpBaseTable)

          LOG.warn(s"Init-Overwrite pluggable base table done", tmpBaseTable)

          sql(s"drop view if exists $view")
          sql(s"drop table if exists $baseTable")
          sql(s"alter table $tmpBaseTable rename to $baseTable")

          sql(s""" set l_time = l_time >= "${currLt}" """)
          sql(
            s"""create view ${view} as
               | select * from $baseTable
               | union all
               | $incrViewSql
          """.stripMargin)

          // 用于记录l_time，要确保后续l_time要大于此前的l_time，否则触发重新初始化
          MC.push(UpdateReq(lastLTimeTopic, currLt))

          true
        }
      }
    )
    true
  }

  def doHandle (): Unit = {
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
          var lastLTimeTopic = s"${view}_lastLTimeTopic"
          var lastLTimeConsumer = s"${view}_lastLTime_cer"

          // Init
          MC.pull(needInitConsumer, Array(needInitTopic), {resp=>

            //有消息有说明需要重新初始化baseTable, 无视消息具体内容
            if(resp.size() > 0) {
              LOG.warn(s"Force-Init base table start", baseTable)
              initAccelerTable(lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
              LOG.warn(s"Force-Init base table done", baseTable)
            }
            true
          })
//          if(!needInit(0)) {//is need init base table
//          }

          // Incr
          // 注意用的是含'Tail'的pullAndSortByLTimeDescTailHivePartitionParts,获取的是排除第一个的l_time(s)
          MC.pullLTimeDescTail(
            consumer,
            topics,
            {l_times =>
              if(l_times.nonEmpty) {

                // 检查l_time是否合法：要确保后续l_time要大于此前的l_time，否则触发重新初始化
                var isIncr = true
                var lastLTime:String = null
                MC.pull(lastLTimeConsumer, Array(lastLTimeTopic), {ms=>
                  if(ms.nonEmpty) {
                    lastLTime = ms.head.getData
                    l_times.foreach { x =>
                      if (isIncr) {
                        isIncr = x.value > lastLTime
                      }
                    }
                  }
                  //首次运行时，lastLTime为空，则需init加速表
                  else if (lastLTime == null) {
                    isIncr = false
                  }

                  //合法l_times,但是base表不存在
                  if(isIncr && sql(s"show tables like '$baseTable'").count() == 0 /*|| sql(s"show tables like '$view'").count() == 0*/) {
                    LOG.warn(s"Re-Create-Base-Init base table start", baseTable)
                    initAccelerTable(lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
                    LOG.warn(s"Re-Create-Base-Init base table done", baseTable)

                  //合法l_times
                  }else if(isIncr) {
                    LOG.warn(s"Incr table  for final dm start", "baseTable", baseTable, "dm", view, "incr l_time(s)", l_times)

                    sql(s"drop view if exists $view")

                    sql(s""" set l_time = l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """)

                    //
                    prevSqls.foreach(x => sql(x))

                    val base = hiveContext.read.table(baseTable)
                    val fs = base.schema.fieldNames

//                    sql(
//                      s"""
//                         |insert overwrite table $baseTable
//                         |select ${fs.mkString(", ")}
//                         |from (
//                         |  ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """)}
//                         |  union all
//                         |  select * from $baseTable
//                         |  where l_time in (${l_times.map(_.value).mkString("'", "', '", "'")})
//                         |)t
//                         |group by ${unionGroup.mkString(", ")}
//                         |
//                   """.stripMargin)

                    sql(incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """))
                      .union(base.where(s" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) "))
                      .groupBy(unionGroup:_*)
                      .agg(
                        unionSelect.head,
                        unionSelect.tail:_*
                      )
                      .select(fs.head, fs.tail:_*)
                      .write
                      .mode(SaveMode.Overwrite)
                      .insertInto(baseTable)

                    LOG.warn(s"Incr-Overwrite base table '${baseTable}' done")

                    var prevLt = l_times.head.value

                    sql(s""" set l_time = l_time > "${prevLt}" """)

                    sql(
                      s"""create view ${view} as
                         | select * from $baseTable
                         | union all
                         | ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time > "${prevLt}" """)}
                    """.stripMargin)

                    // 用于记录l_time，要确保后续l_time要大于此前的l_time，否则触发重新初始化
                    MC.push(UpdateReq(lastLTimeTopic, prevLt))

                    LOG.warn("Incr table  for final dm done")

                  //首次初始化
                  }else if(lastLTime == null){
                    LOG.warn(s"First-Init base table '${baseTable}' start")
                    initAccelerTable(lastLTimeTopic, lastLTime, consumer, topics, prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
                    LOG.warn(s"First-Init base table '${baseTable}' done")


                  //l_times小于lastLTime时（属于非正常情况，因为l_time是时间递增的），需重新init所有数据
                  }else if(!(l_times.head.value.equals(lastLTime))) { //避免相同lastLTime重复被提交时，导致重新init
                    LOG.warn(s"Re-Init base table '${baseTable}' start")
                    initAccelerTable(lastLTimeTopic, lastLTime, consumer, topics, prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
                    LOG.warn(s"Re-Init base table '${baseTable}' done")
                  }

                  false
                })
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

//object AccelerateTableHandlerTest{
//  def main (args: Array[String]): Unit = {
//
//    var l_times = Array("2018-01-01 00:00:00", "2018-01-02 00:00:00", "2017-01-02 00:00:00")
//    var lastLTime = "2019-02-01 00:00:00"
//      (l_times :+ lastLTime).sortBy{x=>x}.reverse.map{x=>println(x)}
//  }
//}

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