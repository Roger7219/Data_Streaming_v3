package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.{CSTTime, MySqlJDBCClientV2, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/8/4.
  */
class PublisherHandler extends Handler{

  private val startMonthBDateFormat = CSTTime.formatter("yyyy-MM-01") //DateFormatUtil.CST("yyyy-MM-01")
  private val endMonthBDateFormat = CSTTime.formatter("yyyy-MM-31") //DateFormatUtil.CST("yyyy-MM-31")
  private val dayBDateFormat = CSTTime.formatter("yyyy-MM-dd") //DateFormatUtil.CST("yyyy-MM-dd")

  var prevDay: String = null
  var prevMonth: String = null

  var monthDmTable: String = null
  var dayDmTable: String = null

  var dwrTotalCostTable: String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient ,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName,bigQueryClient, greenplumClient, rDBConfig,kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    monthDmTable = "ssp_report_overall_dm_month" //"ssp_report_campaign_month_dm"//handlerConfig.getString("dwr.daily.table")
    dayDmTable = "ssp_report_overall_dm_day" // "ssp_report_campaign_dm"

//    dwrTotalCostTable = handlerConfig.getString("dwr.totalcost.table")

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

    LOG.warn(s"PublisherHandler handle start")
    RunAgainIfError.run({
      val p = hiveContext
        .read
  //      .jdbc(rdbUrl, "PUBLISHER", rdbProp)
        .table("PUBLISHER")
        .select("ID", "TodayRevenue", "TodaySendRevenue", "MonthRevenue", "MonthSendRevenue")
        .repartition(1)
        .alias("p")

      LOG.warn("PublisherHandler mysql table PUBLISHER take(10)", p.take(10))

      val now = new Date()
      //当月计费，dwrTotalCostTable.b_date精确到了月份
      val tm = hiveContext
        .read
        .table(monthDmTable) //campaignId
        .where(s"'${startMonthBDateFormat.format(now)}' = b_date ")
//  //      .where(s"b_time = '${monthBDateFormat.format(new Date())}'")
//        .where(s" (data_type = 'camapgin' or data_type is null) and '${startMonthBDateFormat.format(new Date())}' <= b_date  and b_date <= '${endMonthBDateFormat.format(new Date())}' ") //xxx
        .groupBy("publisherId")
        .agg(
          sum("revenue").as("monthSendRevenue"),
          sum("realRevenue").as("monthRevenue")
        )
        .alias("_tm")

      LOG.warn(s"PublisherHandler month revenue take(10)", tm.take(10))

      var today = dayBDateFormat.format(now)

      //当天计费,dwrDailyTable.b_date精确到天
      val d = hiveContext
        .read
        .table(dayDmTable) //campaignId
  //      .where(s"b_time = '${dayBDateFormat.format(new Date())}'")
        .where(s" (data_type = 'camapgin' or data_type is null) and b_date = '$today'") //xxx
        .groupBy("publisherId")
        .agg(
          sum("revenue").as("todaySendRevenue"),
          sum("realRevenue").as("todayRevenue")
        ).alias("_d")

      LOG.warn(s"PublisherHandler day revenue take(10)", d.take(10))

      val up = p
        .join(tm, col("p.ID") === col("_tm.publisherId"), "left_outer")
        .join(d, col("p.ID") === col("_d.publisherId"), "left_outer")
        .selectExpr(
          "p.ID AS publisherId",
          "nvl(_d.todaySendRevenue, 0.0) AS todaySendRevenue",
          "nvl(_d.todayRevenue, 0.0) AS todayRevenue",
          "nvl(_tm.monthSendRevenue, 0.0) AS monthSendRevenue",
          "nvl(_tm.monthRevenue, 0.0) AS monthRevenue"
        )
        .persist()

      LOG.warn(s"PublisherHandler update data take(10)", up.take(10))

      //凌晨当天数据清零
      if(!today.equals(prevDay)) {
        val h = new Date().getHours
        if(h <= 4) {
          LOG.warn("PublisherHandler today data init")
          var sql =
            s"""
               | update PUBLISHER
               | set
               | TodaySendRevenue = 0.0,
               | TodayRevenue = 0.0
                """.stripMargin
          mySqlJDBCClient.execute(sql)
        }
        prevDay = today
      }

      //月初清零当月数据
      var month = startMonthBDateFormat.format(new Date())
      if(!month.equals(prevMonth)) {
        val m = new Date().getDate
        if(m <= 2) {
          LOG.warn("PublisherHandler month data init")
          var sql =
            s"""
               | update PUBLISHER
               | set
               | MonthSendRevenue = 0.0,
               | MonthRevenue = 0.0
                """.stripMargin
          mySqlJDBCClient.execute(sql)
        }
        prevMonth = month
      }


      val ups = up
        .rdd
        .map{x=>
          s"""
             | update PUBLISHER
             | set
             | TodaySendRevenue = ROUND(${x.getAs[Double]("todaySendRevenue")},4),
             | TodayRevenue = ROUND(${x.getAs[Double]("todayRevenue")},4),
             | MonthSendRevenue = ROUND(${x.getAs[Double]("monthSendRevenue")},4),
             | MonthRevenue = ROUND(${x.getAs[Double]("monthRevenue")}, 4)
             | where ID = ${x.getAs[Int]("publisherId")}
           """.stripMargin
        }
        .collect()

      mySqlJDBCClient.executeBatch(ups)

      up.unpersist()

    })

    LOG.warn(s"PublisherHandler handle done")

  }
}
