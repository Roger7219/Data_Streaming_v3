package com.mobikok.ssp.data.streaming.handler.dm.offline

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{MySqlJDBCClientV2, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by Administrator on 2017/9/2.
  */
class AggHandler extends  Handler{

  var dwrTable: String = null   //"agg_traffic_dwr"
  var mysqlTable: String = null //"JAR_ECPM"

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient, greenplumClient:GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    dwrTable = "agg_traffic_month_dwr" //handlerConfig.getString("dwr.table")
    mysqlTable = handlerConfig.getString("mysql.table")
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

  override def handle (): Unit = {
    RunAgainIfError.run{

      val ecpmSql = hiveContext
        .read
        .table(dwrTable)
        .groupBy("jarId", "countryId")
        .agg(expr("if(sum(showCount) > 0, 1000.0*sum(cost)/sum(showCount), 0.0)").as("ecpm"))
        .collect()
        .map{x=>
          s"""
            | update $mysqlTable
            | set
            |   RealEcpm = ${x.getAs[Double]("ecpm")}
            | where
            |   JarId = ${x.getAs[Int]("jarId")}
            |   and CountryId = ${x.getAs[Int]("countryId")}
            |
          """.stripMargin
        }

      LOG.info("AggHandler ecpm update starting", ecpmSql.take(2))
      mySqlJDBCClient.executeBatch(ecpmSql)
      LOG.info("AggHandler ecpm updated", ecpmSql.take(2))
    }
//
//    hiveContext
//      .read
//      .table(dmTable)
//      .groupBy("b_date", "jarId", "appId", "countryId")


  }
}
