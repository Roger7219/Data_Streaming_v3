package com.mobikok.ssp.data.streaming.handler.dm

import java.sql.ResultSet

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
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
  * For SSP 最新版
  */
class SyncMysql2HiveHandlerV2_2 extends Handler {

  var hiveTableDetails: Map[String, (String, String)] = null //[hiveTable(mysqlTable,uuid)]

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null

  val syncIncrMysql2HiveCer = "SyncIncrMysql2HiveCer"
  val syncUpdateMysql2HiveCer = "SyncUpdateMysql2HiveCer"
  var updateWhereTopic = Array("config_update")

  var LAST_ID_CER_PREFIX = "lastIdCerV4"
  var LAST_ID_TOPIC_PREFIX = "lastIdTopicV4"

  var mySqlJDBCClient: MySqlJDBCClientV2 = null
  @volatile var LOCK = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

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

    mySqlJDBCClient = new MySqlJDBCClientV2(
      moduleName, rdbUrl, rdbUser, rdbPassword
    )

    // 如果上次启动时，写入临时表成功，并将hiveT rename to hiveBackupT表了，但未执行rename syncProcessedT to hiveT，则继续尝试rename
    hiveTableDetails.entrySet().foreach { x=>
      val hiveT = x.getKey
      val syncProcessedT = s"${hiveT}_sync_processed"
      if(sql(s"show tables like '$syncProcessedT'").take(1).nonEmpty
        && sql(s"show tables like '$hiveT'").take(1).isEmpty
      ) {
        sql(s"alter table $syncProcessedT rename to ${hiveT}")
      }
    }
  }

  override def handle(): Unit = {
    // 上个批次还在处理时，等待当前批次
    LOCK.synchronized{
      LOG.warn(s"${classOf[SyncMysql2HiveHandlerV2_2].getSimpleName} start")

      hiveTableDetails.entrySet().foreach { x=>
        val hiveT = x.getKey
        val mysqlT = x.getValue._1
        val uuidField = x.getValue._2
        val hiveBackupT = s"${hiveT}_backup"
        val syncProcessingT = s"${hiveT}_sync_processing"
        val syncProcessedT = s"${hiveT}_sync_processed"
        val lastIdCer = s"${LAST_ID_CER_PREFIX}_${hiveT}"
        val lastIdTopic = s"${LAST_ID_TOPIC_PREFIX}_${hiveT}"

        // 待删，初始化的时候操作
        // 如果上次启动时，写入临时表成功，并将hiveT rename to hiveBackupT表了，但未执行rename syncProcessedT to hiveT，则继续尝试rename
        if(sql(s"show tables like '$syncProcessedT'").take(1).nonEmpty
           && sql(s"show tables like '$hiveT'").take(1).isEmpty
        ) {
          sql(s"alter table $syncProcessedT rename to ${hiveT}")
        }

        val hiveDF = hiveContext.read.table(hiveT)

        // 全量刷新
        if (StringUtil.isEmpty(uuidField)) {

          // 检查hive/mysql数据是否一致，一致无需更新
          var mysqlCount = mySqlJDBCClient
            .executeQuery(s"select count(1) from $mysqlT", new Callback[DataFrame] {
              override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
            })
            .first()
            .getAs[Long](0)

          var hiveCount = hiveContext
            .sql(s"select count(1) from $hiveT")
            .first()
            .getAs[Long](0)

          //不一致就需要更新
          if(!hiveCount.equals(mysqlCount)){

            LOG.warn(s"Full table overwrite start", "mysqlTable", mysqlT, "hiveTable", hiveT)

            sql(s"drop table if exists $syncProcessedT")
            sql(s"drop table if exists $syncProcessingT")
            sql(s"create table $syncProcessingT like $hiveT")

            val unc = hiveContext.read.table(syncProcessingT)

            hiveContext
              .read
              .jdbc(rdbUrl, s"$mysqlT as sync_full_table", rdbProp) //需全表查
              .selectExpr(unc.schema.fieldNames.map(x => s"`$x`"): _*)
              .repartition(1)
              .write
              .format("orc")
              .mode(SaveMode.Overwrite)
              .insertInto(syncProcessingT)
            LOG.warn(s"Full table overwrite done", "mysqlTable", mysqlT, "hiveTable", hiveT)

            // 原子性操作，标记写入完成，syncProcessedT表是完整的数据
            sql(s"alter table $syncProcessingT rename to ${syncProcessedT}")

            sql(s"drop table if exists $hiveBackupT")              // 删除上次备份
            sql(s"alter table $hiveT rename to $hiveBackupT")      // 正式表转为备份表
            sql(s"alter table $syncProcessedT rename to ${hiveT}") // 更新表转正

            LOG.warn(s"Full table overwrite done", "mysqlTable", mysqlT, "hiveTable", hiveT)
          }else {
            LOG.warn(s"Full table overwrite cancelled, Because hive and mysql data is consistent", "mysqlTable", mysqlT, "hiveTable", hiveT)
          }

        }
        // 增量刷新
        else {

          LOG.warn(s"Incr table append start", "mysqlTable", mysqlT, "hiveTable", hiveT)
          val incrAndUpdateT = s"${hiveT}_incr_and_update_temp_view"

          // 初始化
          val hiveLastId = sql(s"select cast( nvl( max(id), 0) as bigint) as lastId from $hiveT")
            .first()
            .getAs[Long]("lastId")

          // 检查hive/mysql数据是否一致，不一致无需更新
          var syncedMysqlCount = mySqlJDBCClient
            .executeQuery(s"select count(1) from $mysqlT where id <= $hiveLastId", new Callback[DataFrame] {
              override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
            })
            .first()
            .getAs[Long](0)

          var hiveCount = hiveContext
            .sql(s"select count(1) from $hiveT")
            .first()
            .getAs[Long](0)

          var lastId = 0L

          MC.pull(lastIdCer, Array(lastIdTopic), {x=>
            // hive空表，全量同步
            lastId = if(hiveLastId == 0 ) {
              0L
            }
            // 首次运行，消息队列为空，全量同步
            else if(x.isEmpty) {
              0L
            }
            // 不一致就需要全量同步
            else if(!hiveCount.equals(syncedMysqlCount)) {
              0L
            }
            else {
              x.head.getData.toLong
            }

            LOG.warn("Incr table ","hiveTable", hiveT, "lastId", lastId)

            val fieldsExpr = hiveDF.schema.fieldNames.map(x => s"`$x`")

            val mysqlIncrDF =
              hiveContext
                .read
                .jdbc(rdbUrl, s"(select ${fieldsExpr.mkString(", \n ")} from $mysqlT where id > $lastId) as sync_incr_table", rdbProp)
                .selectExpr(hiveDF.schema.fieldNames.map(x => s"`$x`"): _*)

//            //必须截断式缓存，因为spark是延迟读mysql，可能会读多次，因为mysql的数据一直在变化，所以如果多次读，可能结果不一致
//            val incrDF = hiveContext.createDataFrame(mysqlIncrDF.collectAsList(), mysqlIncrDF.schema)
            // collectAsList() 太耗费drive端内存，放弃
            val incrDF = mysqlIncrDF
            incrDF.persist()

            val updateDF = null // updateDFByWhere(tablesUpdateWhere, mysqlT, hiveT)

            val incrAndUpdateDF = if(updateDF == null) incrDF else incrDF.union(updateDF)

            incrAndUpdateDF.createOrReplaceTempView(incrAndUpdateT)

            if(incrAndUpdateDF.head(1).nonEmpty) {

              // 去重，(确保新值覆盖旧值，暂未实现)
              var uniqueAllDF = sql(
                s"""
                   |select
                   |  ${fieldsExpr.mkString(", \n ")}
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

              //必须缓存，不然延迟读mysql，会现后读两次，导致数据错误
              uniqueAllDF.persist()

              sql(s"drop table if exists $syncProcessedT")
              sql(s"drop table if exists $syncProcessingT")
              sql(s"create table $syncProcessingT like $hiveT")

              uniqueAllDF
                .selectExpr(fieldsExpr: _*)
                .repartition(50)
                .write
                .format("orc")
                .mode(SaveMode.Overwrite)
                .insertInto(syncProcessingT)

              // 原子性操作，标记写入完成，syncProcessedT表是完整的数据
              sql(s"alter table $syncProcessingT rename to ${syncProcessedT}")

              sql(s"drop table if exists $hiveBackupT")              // 删除上次备份
              sql(s"alter table $hiveT rename to $hiveBackupT")      // 正式表转为备份表
              sql(s"alter table $syncProcessedT rename to ${hiveT}") // 更新表转正

              // 记录新的last id
              MC.push(new UpdateReq(lastIdTopic, incrDF
                .selectExpr(s"cast( nvl( max(id), 0) as bigint) as lastId")
                .first()
                .getAs[Long]("lastId").toString
              ))

              uniqueAllDF.unpersist()
            }
            incrDF.unpersist()

            LOG.warn(s"Incr table append done", "mysqlTable", mysqlT, "hiveTable", hiveT)

            false
          })

        }

      }

      LOG.warn(s"${classOf[SyncMysql2HiveHandlerV2_2].getSimpleName} done")
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
              .coalesce(4)
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