package com.mobikok.ssp.data.streaming.handler.dm

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{MessageClient, ModuleTracer, MySqlJDBCClient, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Administrator on 2017/8/4.
  */
class AppHandler extends Handler{

  private val datetimeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val startMonthBDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-01")
  private val endMonthBDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-31")


  var monthDmTable: String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClient = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName,bigQueryClient, rDBConfig, kafkaClient: KafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    monthDmTable = "ssp_report_overall_dm_month"//"ssp_report_campaign_month_dm"//handlerConfig.getString("dwr.table")

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

    mySqlJDBCClient = new MySqlJDBCClient(moduleName, rdbUrl, rdbUser, rdbPassword
    )
  }

  override def doHandle (): Unit = {

    LOG.warn("AppHandler handle start")

    RunAgainIfError.run({
      var now = new Date()
      var st = startMonthBDateFormat.format(now)
//      var et = endMonthBDateFormat.format(now)
      //当月计费，dwrTotalCostTable.b_date精确到了月份
      val f = hiveContext
        .read
        .table(monthDmTable) //campaignId
        .where(s" '$st' = b_date ")
        //.where(s" (data_type = 'camapgin' or data_type is null) and '$st' <= b_date and b_date <= '$et' ")
        .groupBy("appId")
        .agg(
          sum("realRevenue").cast(DoubleType).as("cost")
        )
        .alias("f")

      LOG.warn(s"AppHandler appId/cost take(10)", f.take(10))

      //For serializable
      val _datetimeFormat = datetimeFormat
      val ups = f
        .filter{_.getAs[Double]("cost").compareTo(50) > 0 }
        .rdd
        .map{x=>
          s"""
             |  update APP
             |  set
             |    CommercialTime = "${_datetimeFormat.format(new Date())}"
             |  where ID = ${x.getAs[Int]("appId")} and CommercialTime is null
           """.stripMargin
        }
        .collect()

      mySqlJDBCClient.executeBatch(ups)
    })

    LOG.warn(s"AppHandler handle done")

  }
}
