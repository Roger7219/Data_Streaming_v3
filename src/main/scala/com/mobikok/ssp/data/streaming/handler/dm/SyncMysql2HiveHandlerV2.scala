package com.mobikok.ssp.data.streaming.handler.dm

import java.sql.ResultSet

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient.Callback
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by admin on 2017/9/4.
  */
class SyncMysql2HiveHandlerV2 extends Handler {

  var hiveTableDetails: Map[String, (String, String)] = null //[hiveTable(mysqlTable,uuid)]

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null

  val syncIncrMysql2HiveCer = "SyncIncrMysql2HiveCer"
  val syncUpdateMysql2HiveCer = "SyncUpdateMysql2HiveCer"
  var updateWhereTopic = Array("config_update")

  var mySqlJDBCClient: MySqlJDBCClient = null
  @volatile var lock = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    hiveTableDetails = handlerConfig.getConfigList("tables").map { x =>
      x.getString("hive") -> (x.getString("mysql"), if(x.hasPath("uuid")) x.getString("uuid") else null)
    }.toMap

    LOG.warn("Sync hiveTableDetails", hiveTableDetails)

    rdbUrl = handlerConfig.getString(s"rdb.url")
    rdbUser = handlerConfig.getString(s"rdb.user")
    rdbPassword = handlerConfig.getString(s"rdb.password")

    rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
      }
    }

    mySqlJDBCClient = new MySqlJDBCClient(
      rdbUrl, rdbUser, rdbPassword
    )
  }

  override def doHandle(): Unit = {
    // 上个批次还在处理时，等待当前批次
    lock.synchronized{
      LOG.warn(s"SyncMysql2HiveHandler start")

      MC.pull(syncUpdateMysql2HiveCer, updateWhereTopic, { ms=>

        // 获取修改的有哪些
        val tablesUpdateWhere = mutable.Map[String, ListBuffer[String]]()
        val updateMsg = ms.map { x =>
          OM.toBean(x.getKeyBody, new TypeReference[java.util.List[java.util.Map[String, Object]]] {})
        }.foreach { x =>
          x.foreach { y =>
            val t = y.get("table").asInstanceOf[String]

            val w = y.get("where").asInstanceOf[java.util.Map[String, Object]].entrySet().map { z =>
              s" ${z.getKey} = ${OM.toJOSN(z.getValue)} "
            }.mkString(" (", " AND ", ") ")
//            LOG.warn(s"SyncMysql2HiveHandler table '$t' update where", w)

            var ws = tablesUpdateWhere.get(t)

            if (ws.isEmpty) {
              ws = Some(ListBuffer[String]())
              tablesUpdateWhere.put(t, ws.get)
            }
            ws.get.append(w)
          }
        }

        hiveTableDetails.entrySet().foreach { x=>
          var hiveT = x.getKey
          var mysqlT = x.getValue._1
          var uuidField = x.getValue._2
          var syncResultTmpT = s"${hiveT}_sync_result_tmp"
          var hiveDF = hiveContext.read.table(hiveT)

          // 全量刷新
          if (StringUtil.isEmpty(uuidField/*x.getValue._2*/)) {
            LOG.warn(s"Full table overwrite start", "mysqlTable", mysqlT, "hiveTable", hiveT)
            var unc = hiveContext.read.table(hiveT)

            hiveContext
              .read
              .jdbc(rdbUrl, s"$mysqlT as sync_full_table", rdbProp) //需全表查
              .selectExpr(unc.schema.fieldNames.map(x => s"`$x`"): _*)
              .coalesce(1)
              .write
              .format("orc")
              .mode(SaveMode.Overwrite)
              .insertInto(hiveT)
            LOG.warn(s"Full table overwrite done", "mysqlTable", mysqlT, "hiveTable", hiveT)
          }
          // 增量刷新
          else {

            LOG.warn(s"Incr table append start", "mysqlTable", mysqlT, "hiveTable", hiveT)
            var topic = s"${hiveT}_last_id"
            var incrAndUpdateT = s"${hiveT}_incr_and_update_temp_view"

//            MC.pull(syncIncrMysql2HiveCer, Array(topic), { x =>
              val lastId =
//                if (x.isEmpty) {
                sql(s"select cast( nvl( max(id), 0) as bigint) as lastId from $hiveT")
                  .first()
                  .getAs[Long]("lastId")

//              } else {
//                java.lang.Long.parseLong(x.last.getKeyBody)
//              }

              LOG.warn("Incr table ","hiveTable", hiveT, "lastId", lastId)

              val incrDF =
                /*mySqlJDBCClient
                .executeQuery(s"select * from $mysqlT where id > $lastId", new Callback[DataFrame] {
                  override def onCallback(rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
                })*/
                hiveContext
                  .read
                  .jdbc(rdbUrl, s"(select * from $mysqlT where id > $lastId) as sync_incr_table", rdbProp)
                  .selectExpr(hiveDF.schema.fieldNames.map(x => s"`$x`"): _*)

              val updateDF = updateDFByWhere(tablesUpdateWhere, mysqlT, hiveT)

              var incrAndUpdateDF = if (updateDF == null) incrDF else incrDF.union(updateDF)

            incrAndUpdateDF.createOrReplaceTempView(incrAndUpdateT)
//            incrAndUpdateDF.cache()

            if(incrAndUpdateDF.head(1).nonEmpty) {

              // 去重，(确保新值覆盖旧值，暂未实现)
              var uniqueAllDF = sql(
                s"""
                   |select
                   |  ${hiveDF.schema.fieldNames.map(x => s"`$x`").mkString(", \n  ")}
                   |from(
                   |  select
                   |    *,
                   |    row_number() over(partition by $uuidField order by 1 desc) row_num
                   |  from (
                   |    select * from $incrAndUpdateT
                   |    union all
                   |    select * from $hiveT
                   |  )
                   |)
                   |where row_num = 1
              """.stripMargin)

              sql(s"drop table if exists $syncResultTmpT")
              sql(s"create table $syncResultTmpT like $hiveT")

              uniqueAllDF
                .selectExpr(hiveDF.schema.fieldNames.map(x => s"`$x`"): _*)
                .coalesce(1)
                .write
                .format("orc")
                .mode(SaveMode.Overwrite)
                .insertInto(syncResultTmpT)

              sql(s"drop table if exists ${hiveT}_backup")         // 删除上次备份
              sql(s"alter table $hiveT rename to ${hiveT}_backup") // 备份
              sql(s"alter table $syncResultTmpT rename to ${hiveT}")

//              var lastIdDF = sql(s"select cast(max($uuidFied) as bigint) as lastId from $syncResultTmpT")
//              lastId = if (lastIdDF.head(1).isEmpty) 0L else lastIdDF.first().getAs[Long]("lastId")

//              MC.push(new PushReq(topic, String.valueOf(lastId)))

//              true
//            })

            }
//            incrAndUpdateDF.unpersist()

            LOG.warn(s"Incr table append done", "mysqlTable", mysqlT, "hiveTable", hiveT)

          }

        }

        true
      })

      LOG.warn(s"SyncMysql2HiveHandler done")
    }

  }

    private def updateDFByWhere(tableWheres: mutable.Map[String, ListBuffer[String]], mysqlT: String, hiveT: String): DataFrame ={

      var allWs = tableWheres.getOrElse(mysqlT, ListBuffer[String]())
//      var hiveT = hiveTableDetails.get(mysqlT).get._1
      var df = hiveContext.read.table(hiveT)

      //一个分组里最多50个
      var gSize = 50
      //共有分组数量
      var gCount = Math.ceil(1.0*allWs.size / gSize) ;
      LOG.warn(s"SyncMysql2HiveHandler updateDFByWhere start", "mysqlTable", mysqlT, "hiveTable", hiveT, "allWsSize", allWs.size, "allWs", OM.toJOSN(allWs.asJava))

      var allNeedUpdatedDF: DataFrame = null
      allWs.zipWithIndex.groupBy(y => y._2 % gCount).foreach { z =>
        try {

          var ws = z._2.map(x=>x._1)
          LOG.warn(s"SyncMysql2HiveHandler mysql table '$mysqlT' update where(part)", s"count: ${ws.size}\nwheres: ${OM.toJOSN(ws.asJava)}")

          val updatedDF = mySqlJDBCClient.executeQuery(
            s"""
               |select ${df.schema.fieldNames.map(f=>s"`$f`").mkString(", ")}
               |from $mysqlT
               |where ${ws.mkString(" (", " OR ", ")")}""".stripMargin, new Callback[DataFrame]() {
            override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
          })

          LOG.warn(s"Select mysql updated record", "mysql table", mysqlT, "count", updatedDF.count/*, "take(2)", updatedDf.take(2)*/)

          if(allNeedUpdatedDF == null) {
            allNeedUpdatedDF = updatedDF
          }else {
            allNeedUpdatedDF = allNeedUpdatedDF
              .coalesce(1)
              .union(updatedDF)
          }

        } catch {
          case e: NoSuchTableException =>
            LOG.error(s"Hive table or view '${hiveT}' not exists", e)
        }
      }

      LOG.warn(s"SyncMysql2HiveHandler updateDFByWhere done", "mysqlTable", mysqlT, "hiveTable", hiveT, "allWsSize", allWs.size, "allWs", OM.toJOSN(allWs.asJava))

      allNeedUpdatedDF
    }

}