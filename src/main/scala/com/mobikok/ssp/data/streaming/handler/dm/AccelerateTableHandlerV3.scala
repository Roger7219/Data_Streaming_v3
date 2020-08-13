package com.mobikok.ssp.data.streaming.handler.dm

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
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
class AccelerateTableHandlerV3 extends Handler {
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

  var selectBDateAccExprTemplet: String = null
  var selectBTimeAccExprTemplet:String = null
  var selectLTimeAccExprTemplet:String = null

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
      var topics = Array(sourceTable, s"${view}_re_init")
      if(c.hasPath("topic")) topics :+= c.getString("topic")

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

      // eg: date_format(b_time, yyyy-MM-dd 00:00:00) AS `b_time`, date_format(l_time, yyyy-MM-dd 00:00:00) AS `l_time`, b_date
      // -> date_format(${b_time}, yyyy-MM-dd 00:00:00) AS `b_time`, date_format(${l_time}, yyyy-MM-dd 00:00:00) AS `l_time`, b_date
      val mayPs = Array("b_date","b_time", "l_time")
      unionSelect.foreach{x=>
        val pSelect = x.expr.sql.trim.replaceAll("`b_date`","b_date").replaceAll("`b_time`", "b_time").replaceAll("`l_time`", "l_time")

        mayPs.foreach{p=>
          var pt:String = "SELECT "
          if(pSelect.endsWith(p)) {
            val v = pSelect.substring(0, pSelect.length - p.length)
            if(v.length == 0) {
              pt += s"$${$p} as $p"
            }else{
              pt += v.replaceAll(p, s"\\$$\\{$p\\}") + p
            }

            if(p.equals("b_date")) {
              selectBDateAccExprTemplet = pt
            }else if(p.equals("b_time")) {
              selectBTimeAccExprTemplet = pt
            }else if(p.equals("l_time")) {
              selectLTimeAccExprTemplet =pt
            }
          }

        }
      }
      LOG.warn("AccelerateTableHandler partitions replace templet ",
        "bDateAccExprTemplet", selectBDateAccExprTemplet,
        "bTimeAccExprTemplet", selectBTimeAccExprTemplet,
        "lTimeAccExprTemplet", selectLTimeAccExprTemplet)

      // 首次建表初始化
      var baseTable = s"${view}_base"
      val completedBaseTablePrefix = s"${baseTable}_tmp_completed__"
      var lastLTimeTopic = s"${view}_lastLTimeTopic"
      var lastLTimeConsumer = s"${view}_lastLTime_cer"
      var prevSqls = templateSqls.reverse.tail.reverse // 为空
      var currTable = s"${view}_curr"
      if(sql(s"show tables like '$baseTable'").count() == 0 /*|| sql(s"show tables like '$view'").count() == 0*/) {
        LOG.warn(s"Create-Init base table start", baseTable)
        initAccelerTable(currTable, lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
        LOG.warn(s"Create-Init base table done", baseTable)
      }else {
        //如果上次异常退出，导致没有成功写入baseTable时，重启后补上次的
        overwriteBaseTableByLastCompleted(lastLTimeConsumer, lastLTimeTopic, completedBaseTablePrefix, baseTable, view, currTable)
//        if(sql(s"show tables like '$completedBaseTable'").count() > 0) {
//          sql(s"insert overwrite table $baseTable select * from $completedBaseTable")
//          sql(s"drop table $completedBaseTable")
//          LOG.warn(s"Init-Incr-Overwrite base table '${baseTable}' done")
//        }
      }

      (view, consumer, topics, templateSqls,  unionGroup, unionSelect, createBaseTableSql)
    }.toArray
  }

  def initAccelerTable(currTable: String,lastLTimeTopic:String, lastLTime: String, consumer: String, topics: Array[String], prevSqls: Array[String], baseTable: String, createBaseTableSql: String, incrViewSql: String, view: String): Unit = {
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
          var pervLt: String = null
          if(lastLTime == null) {
            currLt = l_times.head.value
          }else {
            //降序
            currLt = (l_times.map(_.value) :+ lastLTime).sortBy{x=>x}.reverse.head
          }

          // 前1秒时间
          pervLt = CSTTime.time(CSTTime.timeFormatter().parse(currLt).getTime()-1000)

          sql(s"drop table if exists $tmpBaseTable")
          sql(createTmpBaseTableSql)

          sql(s""" set l_time = l_time < "${currLt}" """)

          // 没有任何操作
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

          //sql(s""" set l_time = l_time = "${currLt}" """)

          refreshCurrTable(currTable, incrViewSql, view, Array(currLt), createBaseTableSql, baseTable)

          sql(
            s"""create view if not exists ${view} as
               | select * from $baseTable
               | union all
               | select * from $currTable
          """.stripMargin)

          // 用于记录l_time，要确保后续l_time要大于此前的l_time，否则触发重新初始化
          MC.push(UpdateReq(lastLTimeTopic, pervLt))

          true
        }
      }
    )
    true
  }

  override def doHandle (): Unit = {
    LOG.warn("AccelerateTableHandler handler starting")

      viewConsumerTopics.foreach{ case(view, consumer, topics, templateSqls, unionGroup, unionSelect, createBaseTableSql)=>

//        val (view, consumer, topics, templateSqls, needInit, unionGroup, unionSelect, createBaseTableSql) = x
        RunAgainIfError.run({

          sql("set spark.sql.shuffle.partitions = 1")
          sql("set spark.default.parallelism = 1")

          var baseTable = s"${view}_base"
          val writingTmpBaseTable = s"${baseTable}_tmp_writing"
          val completedBaseTablePrefix = s"${baseTable}_tmp_completed__"
          var incrViewSql = templateSqls.last
          var prevSqls = templateSqls.reverse.tail.reverse // 为空
          var needInitTopic = s"${view}_needInitBaseTable"
          var needInitConsumer = s"${needInitTopic}_cer"
          var lastLTimeTopic = s"${view}_lastLTimeTopic"
          var lastLTimeConsumer = s"${view}_lastLTime_cer"
          var currTable = s"${view}_curr"
          var tmpCurrTable = s"${currTable}_tmp"
          var baseLTimeExpr = unionGroup.filter(_.toString().contains("l_time")).head.expr.sql
          var partitionsUpadteConsumer = consumer + "_for_b_date"

          // Init
          MC.pull(needInitConsumer, Array(needInitTopic), {resp=>

            //有消息有说明需要重新初始化baseTable, 无视消息具体内容
            if(resp.size() > 0) {
              LOG.warn(s"Force-Init base table start", baseTable)
              initAccelerTable(currTable, lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
              LOG.warn(s"Force-Init base table done", baseTable)
            }
            true
          })
//          if(!needInit(0)) {//is need init base table
//          }

          MC.pullLTimeDesc(
            consumer,
            topics,
            {l_times=>
              if(l_times.length > 0) {
                refreshCurrTable(currTable, incrViewSql, view, l_times.map(_.value).toArray, createBaseTableSql, baseTable)

                mergePrevLTimes(
                  l_times.tail, lastLTimeConsumer, lastLTimeTopic, baseTable, consumer, writingTmpBaseTable,
                  prevSqls, currTable, topics, createBaseTableSql, incrViewSql, view,
                  baseLTimeExpr, unionGroup, unionSelect, completedBaseTablePrefix
                )
              }

              true
            }, {(partitionAndMessageMap, descHivePartitionParts)=>
              if(descHivePartitionParts.size()>0) {
                partitionAndMessageMap.remove(descHivePartitionParts.get(0))
              }
              partitionAndMessageMap
            }, {callbackResp: util.List[HivePartitionPart]=>
              callbackResp
            })

          // Incr
          // 注意用的是含'Tail'的pullAndSortByLTimeDescTailHivePartitionParts,获取的是排除第一个的l_time(s)
//          MC.pullLTimeDescTail(
//            consumer,
//            topics,
//            {l_times =>
//
//
//              true
//            }
//          )

          overwriteBaseTableByLastCompleted(lastLTimeConsumer, lastLTimeTopic, completedBaseTablePrefix, baseTable, view, currTable)



//          var b_dates: Array[Array[HivePartitionPart]] = null
//          MC.pullBDateDesc(bDateConsumer, topics, {x=>
//            b_dates = x.map{x=>Array(x)}.toArray
//            if(b_dates != null && b_dates.length > 0) {
//              MC.push(new PushReq(view, OM.toJOSN(b_dates)))
//            }
//            true
//          })

          //b_time、b_date、l_time
          pushMessage(partitionsUpadteConsumer, view, topics)

        })
      }

    LOG.warn("AccelerateTableHandler handler done")
  }

  def mergePrevLTimes(l_times: List[HivePartitionPart], lastLTimeConsumer: String, lastLTimeTopic: String, baseTable: String, consumer:String, writingTmpBaseTable: String,
                      prevSqls:Array[String], currTable: String, topics: Array[String], createBaseTableSql: String, incrViewSql: String, view: String,
                      baseLTimeExpr: String, unionGroup: Array[Column], unionSelect: Array[Column], completedBaseTablePrefix: String): Unit ={

    LOG.warn(s"AccelerateTableHandler try merge prev l_time(s)", l_times)

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
          initAccelerTable(currTable, lastLTimeTopic, null, consumer, topics: Array[String], prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
          LOG.warn(s"Re-Create-Base-Init base table done", baseTable)

          //合法l_times
        }else if(isIncr) {
          LOG.warn(s"Incr table for final dm start", "baseTable", baseTable, "dm", view, "incr l_time(s)", l_times)

          //sql(s"drop view if exists $view")

          sql(s""" set l_time = l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """)

          //
          prevSqls.foreach(x => sql(x))

          sql(s"drop table if exists $writingTmpBaseTable")
          sql(s"create table $writingTmpBaseTable like $baseTable")

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

          // base/incr table: l_time
          var targetTableLTimes = l_times.map{x=>
            sql(s"select ${baseLTimeExpr.replaceAll("`l_time`", s"'${x.value}'")}").take(1)(0).getString(0)
          }
          var lts = targetTableLTimes.mkString("'", "', '", "'")

          sql(s"select * from $currTable where l_time in ($lts)")
            //                    sql(incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time in (${l_times.map(_.value).mkString("'", "', '", "'")}) """))
            .union(base.where(s" l_time in ($lts) "))
            .groupBy(unionGroup:_*)
            .agg(
              unionSelect.head,
              unionSelect.tail:_*
            )
            .select(fs.head, fs.tail:_*)
            //.repartition(2)
            .write
            .mode(SaveMode.Overwrite)
            .insertInto(writingTmpBaseTable)

          var prevLt = l_times.head.value
          val lt = prevLt.replaceAll("-", "___").replaceAll(" ", "__").replaceAll(":","_")

          sql(s"alter table $writingTmpBaseTable rename to ${completedBaseTablePrefix}$lt")
          //                    sql(s"insert overwrite table $baseTable select * from $completedBaseTable")
          //
          //                    LOG.warn(s"Incr-Overwrite base table '${baseTable}' done")
          //

          //
          //                    sql(s""" set l_time = l_time > "${prevLt}" """)
          //
          //                    sql(
          //                      s"""create view if not exists ${view} as
          //                         | select * from $baseTable
          //                         | union all
          //                         | select * from $currTable
          //                    """.stripMargin)
          //                    // ${incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time > "${prevLt}" """)}

          // 用于记录l_time，要确保后续l_time要大于此前的l_time，否则触发重新初始化
          MC.push(UpdateReq(lastLTimeTopic, prevLt))

          LOG.warn("Incr table write completedBaseTable done")

          //首次初始化
        }else if(lastLTime == null){
          LOG.warn(s"First-Init base table '${baseTable}' start")
          initAccelerTable(currTable, lastLTimeTopic, lastLTime, consumer, topics, prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
          LOG.warn(s"First-Init base table '${baseTable}' done")


          //l_times小于lastLTime时（属于非正常情况，因为l_time是时间递增的），需重新init所有数据
        }else if(!(l_times.head.value.equals(lastLTime))) { //避免相同lastLTime重复被提交时，导致重新init
          LOG.warn(s"Re-Init base table '${baseTable}' start")
          initAccelerTable(currTable, lastLTimeTopic, lastLTime, consumer, topics, prevSqls, baseTable, createBaseTableSql, incrViewSql, view)
          LOG.warn(s"Re-Init base table '${baseTable}' done")
        }

        false
      })
    }
  }

  def overwriteBaseTableByLastCompleted(lastLTimeConsumer: String, lastLTimeTopic:String, completedBaseTablePrefix: String, baseTable: String, view: String, currTable: String): Unit ={

    val lastLTime = MC.pullUpdatable(lastLTimeConsumer, Array(lastLTimeTopic))

    sql(s"show tables like '${completedBaseTablePrefix}*'")
      .collect
      .foreach{x=>
        val tn = x.getAs[String]("tableName")
        val lt = tn
          .substring(completedBaseTablePrefix.length)
          .replaceAll("___", "-")
          .replaceAll("__", " ")
          .replaceAll("_", ":")



        if(StringUtil.notEmpty(lastLTime) && StringUtil.notEmpty(lt) && lastLTime.equals(lt)) {

          sql(s"insert overwrite table $baseTable select * from $tn")
          LOG.warn(s"Incr-Overwrite base table '${baseTable}' from $tn done")
          sql(
            s"""create view if not exists ${view} as
               | select * from $baseTable
               | union all
               | select * from $currTable
              """.stripMargin)
          LOG.warn("Incr table  for final dm done")
        }
        sql(s"drop table $tn")
      }
  }

  def pushMessage(consumer: String, view: String, topics: Array[String]): Unit ={
    MC.pull(consumer, topics, {ms=>
      ms.foreach{m=>
        val pss = OM.toBean(m.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]() {})
        //              val accPss = new util.ArrayList[util.ArrayList[HivePartitionPart]]()

        var accPss = pss.map{x=>
          x.map{y=>
            val n = y.getName
            val v = y.getValue

            var e: String = null

            if("b_date".equals(n) && selectBDateAccExprTemplet != null && !"__HIVE_DEFAULT_PARTITION__".equals(v) && StringUtil.notEmpty(v)) {
              e = selectBDateAccExprTemplet.replaceAll("""\$\{b_date\}""", s""" "$v" """)
            }else if("b_time".equals(n) && selectBTimeAccExprTemplet != null && !"__HIVE_DEFAULT_PARTITION__".equals(v) && StringUtil.notEmpty(v)) {
              e = selectBTimeAccExprTemplet.replaceAll("""\$\{b_time\}""", s""" "$v" """)
            }else if("l_time".equals(n) && selectLTimeAccExprTemplet != null && !"__HIVE_DEFAULT_PARTITION__".equals(v) && StringUtil.notEmpty(v)) {
              e = selectLTimeAccExprTemplet.replaceAll("""\$\{l_time\}""", s""" "$v" """)
            }

            if(e != null) {
              HivePartitionPart(n, sql(e).first().getAs[String](0))
            }else {
              null
            }

          }.filter(_ != null)

        }

        MC.push(new PushReq(view, OM.toJOSN(accPss)))

      }

      true
    })

  }

  def refreshCurrTable (currTable: String, incrViewSql:String, view: String, currLTimes: Array[String], createBaseTableSql: String, baseTable: String): Unit ={
    if(currLTimes.length>0) {
      LOG.warn("AccelerateTableHandler refreshCurrTable", currLTimes)
      var tmpCurrTable = s"${currTable}_tmp"

      sql(s"drop table if exists $tmpCurrTable")
      var createTmpCurrTableSql = createBaseTableSql.replaceAll(baseTable, tmpCurrTable)
      sql(createTmpCurrTableSql)


      sql(incrViewSql.replaceAll("""\$\{l_time\}""", s""" l_time in (${currLTimes.mkString("'", "', '", "'")})"""))
        .write
        .mode(SaveMode.Overwrite)
        .insertInto(tmpCurrTable)
      sql(s"drop table if exists $currTable")
      sql(s"alter table $tmpCurrTable rename to $currTable")
    }
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }
}



//object AccelerateTableHandlerTest2{
//  def main (args: Array[String]): Unit = {
//
//    println(CSTTime.time(CSTTime.timeFormatter().parse("2018-03-01 00:00:00").getTime()-1000))
//
////    var l_times = Array("2018-01-01 00:00:00", "2018-01-02 00:00:00", "2017-01-02 00:00:00")
////    var lastLTime = "2019-02-01 00:00:00"
////      (l_times :+ lastLTime).sortBy{x=>x}.reverse.map{x=>println(x)}
//  }
//}

//
//object x222{
//  def main (args: Array[String]): Unit = {
//
//    var pt3:String = "SELECT "
//    pt3+="ss"
//    println(pt3)
//
//    val p = "b_time"
//    val pt = s"""$${$p} as $p"""
//    println(pt)
//
//
//    println( s"$${${p}} as $p")
//    var pt2 =pt.replaceAll(p, s"\\$$\\{$p\\}")
//    println(pt2)
//    //
////    var topics = Array("asd", s"_re_init")
////    if(true) topics :+= "weee"
////
////    println(topics.toList)
////    val vv= """
////      |date_format(b_date, 'yyyy-MM-01')|
////      |            publisherid-
////      |
////    """.stripMargin
////    println(vv.split("""\|""")(0))
//  }
//}


//object AccTest{
//
//  def main(args: Array[String]): Unit = {
//    MC.init(new MessageClient("http://node14:5555"))
//    MC.pullLTimeDesc(
//      "ssp_topn_dm_cer33",
//      Array("ssp_report_overall_dwr_day", "ssp_report_overall_dwr_day_test"),
//      {l_times=>
//
//        println("l_times:" + l_times)
//        true
//      }, {(partitionAndMessageMap, descHivePartitionParts)=>
//        if(descHivePartitionParts.size()>0) {
//          partitionAndMessageMap.remove(descHivePartitionParts.get(0))
//        }
//        partitionAndMessageMap
//      }, {callbackResp: util.ArrayList[HivePartitionPart]=>
//
////        if(callbackResp.size() > 0) {
////          callbackResp.remove(0);
////        }
//        callbackResp
//      })
//    println("the end")
//  }
//}