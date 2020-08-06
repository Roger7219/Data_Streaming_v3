package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * For ADX
  */
class SyncMysql2HiveHandlerV3 extends Handler {

  var tableDetails: Map[String, (String, String)] = null //[mysql(hive,uuid)]

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null

  val syncIncrMysql2HiveCer = "SyncIncrMysql2HiveCer"
  val syncUpdateMysql2HiveCer = "SyncUpdateMysql2HiveCer"
  var updateWhereTopic = Array("config_update")

  var mySqlJDBCClient: MySqlJDBCClientV2 = null
  @volatile var lock = new Object()

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    tableDetails = handlerConfig.getConfigList("tables").map { x =>
      x.getString("mysql") -> (x.getString("hive"), if(x.hasPath("uuid")) x.getString("uuid") else null)
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
    // 上个批次还在处理时，等待当前批次
    lock.synchronized{
      LOG.warn(s"SyncMysql2HiveHandler start")

      tableDetails.entrySet().foreach { x =>
        var mysqlT = x.getKey;
        var hiveT = x.getValue._1;
        var syncResultTmpT = s"${hiveT}_sync_result_tmp"

        val last_updated_time =
          sql(s"select max(last_updated_time) as last_updated_time from $hiveT")
            .first()
            .getAs[String]("last_updated_time")
        LOG.warn("Incr table ","hiveTable", hiveT, "last_updated_time", last_updated_time)
        val updateCountDF =
          hiveContext
            .read
            .jdbc(rdbUrl, s"(select count(1) as count from $mysqlT where last_updated_time > '$last_updated_time') as sync_update_table", rdbProp)
            .first()
            .getAs[Long]("count")

        // 全量刷新
        if (updateCountDF > 0) {
          sql(s"drop table if exists $syncResultTmpT")
          sql(s"create table $syncResultTmpT like $hiveT")

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
            .insertInto(syncResultTmpT)
          sql(s"drop table if exists ${hiveT}_backup")         // 删除上次备份
          sql(s"alter table $hiveT rename to ${hiveT}_backup") // 备份
          sql(s"alter table $syncResultTmpT rename to ${hiveT}")
          LOG.warn(s"Full table overwrite done", "mysqlTable", mysqlT, "hiveTable", hiveT)

        }
      }

      LOG.warn(s"SyncMysql2HiveHandler done")
    }

  }

}