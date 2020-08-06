package com.mobikok.ssp.data.streaming.handler.dm

import java.sql.ResultSet

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by admin on 2017/9/4.
  */
class SyncMysql2HiveHandler extends Handler {


  var tables: Array[(String, String, Boolean)] = null //mysql/hive/isIncr
  var mysqlHiveTableMap: Map[String, String] = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null
  val topicPrefix = "sync_"
  val needIncrSyncTopicPrefix = "need_sync_"
  val lastIdIncrConsumer = "SyncMysql2HiveHandler"
  val configUpdateConsumer = "ConfigUpdateConsumer"
  var needIncrConsumer = "NeedIncrConsumer"

  var lastIncrSyncMS = CSTTime.now.ms()
  var mySqlJDBCClient: MySqlJDBCClientV2 = null
  //  val tableSuffix = "_test"

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    tables = handlerConfig.getConfigList("tables").map { x =>
      (x.getString("mysql"), x.getString("hive"), x.getBoolean("isIncr"))
    }.toArray

    mysqlHiveTableMap = handlerConfig.getConfigList("tables").map { x =>
      x.getString("mysql") -> x.getString("hive")
    }.toMap

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
  }


  override def handle(): Unit = {

    //解決lastId之前的數據更新后，不能同步至hive的問題
    val pd = messageClient
      .pullMessage(new MessagePullReq(configUpdateConsumer, Array(s"config_update")))
      .getPageData

    // mysql_table, wheres
    val cm = mutable.Map[String, ListBuffer[String]]()
    val updateMsg = pd.map { x =>
      OM.toBean(x.getKeyBody, new TypeReference[java.util.List[java.util.Map[String, Object]]] {} /*classOf[List[Object]]*/)
    }.foreach { x =>
      x.foreach { y =>
        val t = y.get("table").asInstanceOf[String]

        val w = y.get("where").asInstanceOf[java.util.Map[String, Object]].entrySet().map { z =>
          s" ${z.getKey} = ${OM.toJOSN(z.getValue)} "
        }.mkString(" (", " AND ", ") ")
        LOG.warn(s"SyncMysql2HiveHandler table '$t' where conditions", w)

        var ws = cm.get(t)

        if (ws.isEmpty) {
          ws = Some(ListBuffer[String]())
          cm.put(t, ws.get)
        }
        ws.get.append(w)
      }
    }

    //一个分组里最多50个
    var gSize = 50
    //共有分组数量
    var gCount = cm.size / gSize

    cm.entrySet().foreach { x =>
      var t = x.getKey
      var allWs = x.getValue
      //一个分组里最多20个
      var gSize = 20;
      //共有分组数量
      var gCount = Math.ceil(1.0*allWs.size / gSize) ;

      LOG.warn(s"SyncMysql2HiveHandler table '$t' update where(all)", s"count: ${allWs.size}\nwheres: ${OM.toJOSN(allWs.asJava)}")

      allWs.zipWithIndex.groupBy(y => y._2 % gCount).foreach { z =>
        try {
          var ht = mysqlHiveTableMap.get(t)
          if(ht.isDefined) {
            var ws = z._2.map(x=>x._1)
            LOG.warn(s"SyncMysql2HiveHandler table '$t' update where(part)", s"count: ${ws.size}\nwheres: ${OM.toJOSN(ws.asJava)}")

//            val updatedDf = hiveContext
//              .read
//              .jdbc(rdbUrl, t, rdbProp)
//              .where(ws.mkString(" (", " OR ", ")"))
            val updatedDf = mySqlJDBCClient.executeQuery(s"select * from $t where ${ws.mkString(" (", " OR ", ")")}", new Callback[DataFrame]() {
              override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
            })

            LOG.warn(s"SyncMysql2HiveHandler read mysql updated record", "table", t, "count", updatedDf.count/*, "take(2)", updatedDf.take(2)*/)

            val unchanged = hiveContext.read.table(t).where(ws.mkString(" ( not ", " AND not ", ")"))
            LOG.warn(s"SyncMysql2HiveHandler change-union-unchange overwrite start",
              "table", ht.get,
              "change-count", updatedDf.count(),
              "unchanged-count", unchanged.count,
              "updated-take(2)", updatedDf.take(2)
            )

            unchanged
              .union(updatedDf.selectExpr(unchanged.schema.fieldNames.map(x=>s"`$x`"):_*))
              .coalesce(1)
              .write
              .mode(SaveMode.Overwrite)
              .insertInto(ht.get)
          }else{
            LOG.warn(s"Hive table or view '${t}' not configured in the configuration file")
          }
        } catch {
          case e: NoSuchTableException =>
            LOG.warn(s"Hive table or view '${t}' not exists")
        }
      }
    }

    messageClient.commitMessageConsumer(
      pd.map {d=>
        new MessageConsumerCommitReq(configUpdateConsumer, d.getTopic, d.getOffset)
      }:_*
    )

    //新增数据同步到hive
    tables.foreach { x =>

      val hiveTable = x._2
      val mysqlTable = x._1
      var isIncr = x._3

      //增量导入
      if (isIncr) {

        MC.pull(needIncrConsumer, Array(s"${needIncrSyncTopicPrefix}${hiveTable}"), {x=>
           // 至少20分钟更新一次
          if(x.length > 0 ||CSTTime.now.ms() - lastIncrSyncMS >= 1000*60*20) {

            incrSyncTable(hiveTable, mysqlTable)
            lastIncrSyncMS = CSTTime.now.ms()

          }
          true
        })
      } else {

        LOG.warn(s"SyncMysql2HiveHandler full-overwrite table $hiveTable from mysql")
        //全量导入
        var unc = hiveContext.read.table(hiveTable)
        hiveContext
          .read
          .jdbc(rdbUrl, s"$mysqlTable as sync_full_table1", rdbProp) //需全表查
          .selectExpr(unc.schema.fieldNames.map(x=>s"`$x`"):_*)
          .coalesce(1)
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(hiveTable)

      }

      //greenplumClient.overwrite(t, t, s"${t}_default_partition")

    }

    LOG.warn("SyncMysql2HiveHandler handler done")
  }

  def incrSyncTable(hiveTable: String, mysqlTable: String): Unit ={
    val datas = messageClient
      .pullMessage(new MessagePullReq(
        lastIdIncrConsumer,
        Array(s"${topicPrefix}${hiveTable}")
      ))
      .getPageData

    val lastIds = datas.map { x =>
      x.getTopic -> java.lang.Long.parseLong(x.getData)
    }.toMap

    LOG.warn("SyncMysql2HiveHandler pull message, lastIds", lastIds.asJava)

    var lastId = lastIds.getOrElse(s"${topicPrefix}${hiveTable}", -1)

    var unc = hiveContext.read.table(hiveTable)

    //        hiveContext
    //          .read
    //          .jdbc(rdbUrl, mysqlTable, rdbProp)
    //          .filter(s"id > $lastId")
    //          .selectExpr(unc.schema.fieldNames:_*)
    val incr = mySqlJDBCClient
      .executeQuery(s"select * from $mysqlTable where id > $lastId", new Callback[DataFrame] {
        override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
      })
      .selectExpr(unc.schema.fieldNames.map(x=>s"`$x`"):_*)

    LOG.warn(s"SyncMysql2HiveHandler incr-union-unchange overwrite start", "hiveTable", hiveTable, "mysqlTable", mysqlTable, "incr-count", incr.count(), "unchanged-count", unc.count, "incr-take(2)", incr.take(2))
    if(incr.count() > 0) {
      incr
        .union(unc)
        // .distinct()
        .coalesce(1)
        .write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .insertInto(hiveTable)

      //检查是否重复
      var duplicates = 0L;
      var c = hiveContext
        .sql(s"select count(1) as count from $hiveTable group by id order by 1 desc limit 1")
        .collect()
      if (c.length > 0) {
        duplicates = c(0).getAs[Long]("count")
      }

      LOG.warn(s"SyncMysql2HiveHandler check table $hiveTable duplicate, Duplicate count", s"$duplicates")

      //检查mysql和hive表中条目数是否相等
      var hiveCount = hiveContext
        .sql(s"select count(1) from $hiveTable")
        .first()
        .getAs[Long](0)


      var newLastId:Long = 0

      if(incr.count() > 0) {
        newLastId = incr.agg(expr("cast(max(id) as bigint) as lastId")).first().getAs[Long]("lastId")
      }else {
        newLastId = hiveContext
          .read
          .table(hiveTable)
          .agg(expr("cast(max(id) as bigint) as lastId"))
          .first()
          .getAs[Long]("lastId")
      }

      //        var mysqlCount = hiveContext
      //          .read
      //          .jdbc(rdbUrl, mysqlTable, rdbProp)
      //          .selectExpr("count(1)")
      var mysqlCount = mySqlJDBCClient
        .executeQuery(s"select count(1) from $mysqlTable where id <= $newLastId", new Callback[DataFrame] {
          override def onCallback (rs: ResultSet): DataFrame = OM.assembleAsDataFrame(rs, hiveContext)
        })
        .first()
        .getAs[Long](0)

      val sameCount = (hiveCount == mysqlCount)

      if (duplicates > 1 || !sameCount) {

        LOG.warn(s"SyncMysql2HiveHandler check out inconsistent, full table overwrite start", "mysqlTable", mysqlTable, "hiveTable", hiveTable, "duplicates", duplicates, "hiveCount", hiveCount, "mysqlCount", mysqlCount)
        var unc = hiveContext.read.table(hiveTable)

        hiveContext
          .read
          .jdbc(rdbUrl,  s"$mysqlTable as sync_full_table0", rdbProp) //需全表查
          .selectExpr(unc.schema.fieldNames.map(x=>s"`$x`"):_*)
          .coalesce(1)
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(hiveTable)
        LOG.warn(s"SyncMysql2HiveHandler check out inconsistent, full table overwrite done")
      }

      //提交lastId
      lastId = newLastId/*hiveContext
            .read
            .table(hiveTable)
            .agg(expr("cast(max(id) as bigint) as lastId"))
            .first()
            .getAs[Long]("lastId")*/

      lastId = if (lastId == null) -1 else lastId

      //提交新的lastId
      messageClient.pushMessage(new MessagePushReq(
        s"${topicPrefix}${hiveTable}",
        "-",
        true,
        String.valueOf(lastId)
      ))
    }

    val ofs = datas.map { x =>
      new MessageConsumerCommitReq(lastIdIncrConsumer, x.getTopic, x.getOffset)
    }
    messageClient.commitMessageConsumer(ofs.toArray: _*)
  }
}


//object X{
//  def main(args: Array[String]): Unit = {
//
//    val list = (0 until 100).map{x=> "--"}
//
//    //一个分组里最多50个
//    var gSize = 20
//    //共有分组数量
//    var gCount = list.size/gSize
//
//    list.zipWithIndex.groupBy{x=>x._2%gCount}.foreach{y =>
//      println(y)
//
//    }
//
//  }
//}