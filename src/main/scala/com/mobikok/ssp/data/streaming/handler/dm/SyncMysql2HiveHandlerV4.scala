package com.mobikok.ssp.data.streaming.handler.dm

import java.sql.ResultSet

import com.mobikok.message.client.MessageClientApi
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
  * For SDK动态加载
  */
class SyncMysql2HiveHandlerV4 extends Handler {

  //case(hiveT, (mysqlT, uuidField, incrField, defaultIncrValueIfNull)
  var hiveTableDetails: Map[String, (String, String, String, Any)] = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null

  val syncIncrMysql2HiveCer = "SyncIncrMysql2HiveCer"
  val syncUpdateMysql2HiveCer = "SyncUpdateMysql2HiveCer"
  var updateWhereTopic = Array("config_update")

  var LAST_ID_CER_PREFIX = "lastIdCerV4"
  var LAST_ID_TOPIC_PREFIX = "lastIdTopicV4"

  var mySqlJDBCClient: MySqlJDBCClient = null
  @volatile var LOCK = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    hiveTableDetails = handlerConfig.getConfigList("tables").map { x =>

      val defaultIncrValueIfNull = if(x.hasPath("incr")) {

        val hiveT = x.getString("hive")
        val incr = x.getString("incr")
        val t = hiveContext.read.table(hiveT).schema.fields.map{x=>(x.name->x)}.toMap
          .get(incr)
          .get
          .dataType
          .simpleString
          .toLowerCase

        if("string".equals(t)) {
          "\"\""
        }else if("int".equals(t) || "bigint".equals(t)) {
          0L
        }else {
          throw new RuntimeException("Unsupported incr field type: " + t +", incr field: "+ incr + ", hive table: " + hiveT)
        }
      }else{
        null
      }

      x.getString("hive") -> (
        x.getString("mysql"),
        if(x.hasPath("uuid")) x.getString("uuid") else null,
        if(x.hasPath("incr")) x.getString("incr") else null,
        defaultIncrValueIfNull)
    }.toMap

    //检查设置,uuid和incr要么都设置，要么都不设置
    hiveTableDetails.foreach{case(_, (_, uuid, incr, _))=>
      if(!((StringUtil.isEmpty(incr) && StringUtil.isEmpty(uuid)) || (StringUtil.notEmpty(incr) && StringUtil.notEmpty(uuid)))) {
        throw new RuntimeException("The 'uuid' and 'incr' must both set value or both not, Current uuid: " + uuid  +", incr: " + incr)
      }

    }

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
      moduleName, rdbUrl, rdbUser, rdbPassword
    )

    //    Only for test
    //    # spark-shell --jars file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar
    //
    //    var rdbUrl = "jdbc:mysql://mysql.nadx.iifab.com:3306/nadx?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"
    //    var rdbProp = new java.util.Properties {
    //      {
    //        setProperty("user", "root")
    //        setProperty("password", "MPudKqs79V5ZWPHS")
    //        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
    //      }
    //    }
    //    import org.apache.spark.sql.hive.HiveContext
    //    val hc = new HiveContext(sc)
    //    var df = hc.read.jdbc(rdbUrl, s"(select * from tb_supply_account) as sync_incr_table", rdbProp)
    //    df.selectExpr("max(last_updated_time)").printSchema()

  }

  override def doHandle(): Unit = {
    // 上个批次还在处理时，等待当前批次
    LOCK.synchronized{
      LOG.warn(s"${classOf[SyncMysql2HiveHandlerV4].getSimpleName} start")

      hiveTableDetails.foreach{ case(hiveT, (mysqlT, uuidField, incrField, defaultIncrValueIfNull)) =>
        val hiveBackupT = s"${hiveT}_backup"
        val syncProcessingT = s"${hiveT}_sync_processing"
        val syncProcessedT = s"${hiveT}_sync_processed"
        val lastIncrCer = s"${LAST_ID_CER_PREFIX}_${hiveT}"
        val lastIncrTopic = s"${LAST_ID_TOPIC_PREFIX}_${hiveT}"

        // 如果上次写入临时表成功，并将hiveT rename to hiveBackupT表了，但未执行rename syncProcessedT to hiveT，则继续尝试rename
        if(sql(s"show tables like '$syncProcessedT'").take(1).nonEmpty
           && sql(s"show tables like '$hiveT'").take(1).isEmpty
        ) {
          sql(s"alter table $syncProcessedT rename to ${hiveT}")
        }

        val hiveDF = hiveContext.read.table(hiveT)

        // 全量刷新
        if (StringUtil.isEmpty(incrField)) {
          LOG.warn(s"Full table overwrite start", "mysqlTable", mysqlT, "hiveTable", hiveT)

          //写入hive
          insertOverwriteTable(hiveT, hiveContext
            .read
            .jdbc(rdbUrl, s"$mysqlT as sync_full_table", rdbProp) //需全表查
            .coalesce(1)
          )

          LOG.warn(s"Full table overwrite done", "mysqlTable", mysqlT, "hiveTable", hiveT)
        }
        // 增量刷新
        else {

          LOG.warn(s"Incr table append start", "mysqlTable", mysqlT, "hiveTable", hiveT)

          messageClient.pull(lastIncrCer, Array(lastIncrTopic), { x=>

            val hiveTableIsEmpty = sql(s"select 1 from from $hiveT limit 1").take(1).isEmpty

            var lastIncr: Any = if(hiveTableIsEmpty) {// hive空表，全量同步
              null
            } else if(x.isEmpty) {                    // 首次运行，消息队列为空，全量同步
              null
            } else {
              x.head.getData                          // json格式的，例如时间格式化的字符串："2010-12-12 00:00:00"（注意两边包含引号）
            }

            lastIncr = if(lastIncr != null) lastIncr else defaultIncrValueIfNull

            // 验证，不能为null值
            if(lastIncr == null) throw new RuntimeException("lastIncr value cannot be null")
            LOG.warn("Incr table ","hiveTable", hiveT, "lastIncr", lastIncr)

            val mysqlIncrDF =
              hiveContext
                .read
                // 用>=，因为incrField是精确到秒的时间，因此可能有相同的时间，导致下次可能漏掉这部分数据
                .jdbc(rdbUrl, s"(select * from $mysqlT where $incrField >= $lastIncr) as sync_incr_table", rdbProp)
                .selectExpr(hiveDF.schema.fieldNames.map(x => s"`$x`"): _*)

            //必须截断式缓存，因为spark是延迟读mysql，可能会读多次，因为mysql的数据一直在变化，所以如果多次读，可能结果不一致
            val incrDF = hiveContext.createDataFrame(mysqlIncrDF.collectAsList(), mysqlIncrDF.schema)
            incrDF.persist()

            val updateDF = null // updateDFByWhere(tablesUpdateWhere, mysqlT, hiveT)

            val incrAndUpdateDF = if(updateDF == null) incrDF else incrDF.union(updateDF)

            val incrAndUpdateT = s"${hiveT}_incr_and_update_temp_view"
            incrAndUpdateDF.createOrReplaceTempView(incrAndUpdateT)

            if(incrAndUpdateDF.head(1).nonEmpty) {

              // 去重，(确保新值覆盖旧值，暂未实现)
              var uniqueAllDF = sql(
                s"""
                   |select
                   |  ${hiveDF.schema.fieldNames.map(x => s"`$x`").mkString(", \n  ")}
                   |from(
                   |  select
                   |    *,
                   |    row_number() over(partition by $uuidField order by $incrField desc) row_num
                   |  from (
                   |    select * from $incrAndUpdateT
                   |    union all
                   |    select * from $hiveT
                   |  )
                   |)
                   |where row_num = 1
            """.stripMargin)

              // 必须缓存，不然延迟读mysql，会现后读两次，导致数据错误
              uniqueAllDF.persist()

              //写入hive
              insertOverwriteTable(hiveT, uniqueAllDF)

              // 记录新的last id
              messageClient.push(new UpdateReq(lastIncrTopic, OM.toJOSN(incrDF
                .selectExpr(s"max($incrField) as lastIncr")
                .first()
                .getAs[Object]("lastIncr"))
              ))

              uniqueAllDF.unpersist()
            }
            incrDF.unpersist()

            LOG.warn(s"Incr table append done", "mysqlTable", mysqlT, "hiveTable", hiveT)

            false
          })

        }

      }

      LOG.warn(s"${classOf[SyncMysql2HiveHandlerV4].getSimpleName} done")
    }

  }

  private def insertOverwriteTable(hiveT: String, df: DataFrame): Unit ={

    val hiveBackupT = s"${hiveT}_backup"
    val syncProcessingT = s"${hiveT}_sync_processing"
    val syncProcessedT = s"${hiveT}_sync_processed"

    sql(s"drop table if exists $syncProcessedT")
    sql(s"drop table if exists $syncProcessingT")
    sql(s"create table $syncProcessingT like $hiveT")

    df
      .selectExpr(hiveContext.read.table(hiveT).schema.fieldNames.map(x => s"`$x`"): _*)
      .coalesce(4)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .insertInto(syncProcessingT)

    // 原子性操作，标记写入完成，syncProcessedT表此时是完整的数据
    sql(s"alter table $syncProcessingT rename to ${syncProcessedT}")

    sql(s"drop table if exists $hiveBackupT")              // 删除上次备份
    sql(s"alter table $hiveT rename to $hiveBackupT")      // 正式表转为备份表
    sql(s"alter table $syncProcessedT rename to ${hiveT}") // 更新表转正

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