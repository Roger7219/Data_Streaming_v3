package com.mobikok.ssp.data.streaming.handler.dm

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/8/4.
  */
class CampaignAdverHandler extends Handler{

  private val dayBDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-dd") //new SimpleDateFormat("yyyy-MM-dd")
  private val startMonthBDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-01") //new SimpleDateFormat("yyyy-MM-01")
//  private val endMonthBDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-31") // new SimpleDateFormat("yyyy-MM-31")

  var prevDay: String = null
  var prevMonth: String = null

  var monthDmTable: String = null
  var dayDmTable: String = null

  //  var dwrTotalCostTable: String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClient = null

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName,bigQueryClient, rDBConfig,kafkaClient: KafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    monthDmTable = "ssp_report_overall_dm_month" //"ssp_report_campaign_month_dm" //handlerConfig.getString("dwr.daily.table")
    dayDmTable = "ssp_report_overall_dm_day" //"ssp_report_campaign_dm"
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

    mySqlJDBCClient = new MySqlJDBCClient(
      moduleName, rdbUrl, rdbUser, rdbPassword
    )
  }

  override def doHandle (): Unit = {

//    from ssp_click_dwr c
//    left join campaign ca on c.campaignId = ca.id
//    left join advertiser ad on ad.id = ca.adverid
    RunAgainIfError.run({
      val a = hiveContext
        .read
        .table("ADVERTISER")
//        .jdbc(rdbUrl, "ADVERTISER", rdbProp)
        .select("ID", "TodayCost", "MonthCost", "TodayCost")
//        .repartition(1)
        .alias("a")

      LOG.warn("AdverHandler mysql table ADVERTISER take(10)", a.take(10))

      val c = hiveContext
        .read
//        .jdbc(rdbUrl, "CAMPAIGN", rdbProp)
        .table("CAMPAIGN")
        .select("ID", "AdverId","ShowCount", "ClickCount", "DailyCost", "TotalCost")
//        .repartition(1)
        .alias("c")

      LOG.warn("AdverHandler mysql table CAMPAIGN take(10)", c.take(10))


      //总计费，dwrTotalCostTable.b_date精确到了月份
      val t = hiveContext
        .read
        .table(monthDmTable) //campaignId
//        .where("(data_type = 'camapgin' or data_type is null)")
        .alias("tm")
        .join(c, col("tm.campaignId") === col("c.ID"), "left_outer")
        .selectExpr(
          "c.AdverId as adverId",
          "CAST(tm.realRevenue AS DOUBLE) as totalCost"
        )
        .groupBy("adverId")
        .agg(sum("totalCost").as("totalCost"))
        .alias("_t")
      t.persist()

      LOG.warn(s"AdverHandler totalCost take(10)", t.take(10))

      val now = new Date()

      //当月计费，dwrTotalCostTable.b_date精确到了月份
      val tm = hiveContext
        .read
        .table(monthDmTable) //campaignId
        .where(s" '${startMonthBDateFormat.format(now)}' = b_date ")
        //.where(s" (data_type = 'camapgin' or data_type is null) and '${startMonthBDateFormat.format(now)}' <= b_date  and b_date <= '${endMonthBDateFormat.format(now)}' ")
        .alias("tm")
        .join(c, col("tm.campaignId") === col("c.ID"), "left_outer")
        .selectExpr(
  //        "tc.b_date",
          "c.AdverId as adverId",
          "CAST(tm.realRevenue as DOUBLE) as totalCost"
        )
        .groupBy("adverId")
        .agg(sum("totalCost").as("monthCost"))
        .alias("_tm")
      tm.persist()

      LOG.warn(s"AdverHandler monthCost take(10)", tm.take(10))

      //当天计费,dwrDailyTable.b_date精确到天
      val d = hiveContext
        .read
        .table(dayDmTable) //campaignId
        .where(s" (data_type = 'camapgin' or data_type is null) and b_date = '${dayBDateFormat.format(now)}'")
        .alias("d")
        .join(c, col("d.campaignId") === col("c.ID"), "left_outer")
        .selectExpr(
  //        "d.b_date",
          "c.AdverId as adverId",
          "CAST(d.realRevenue as DOUBLE) as dailyCost"
        )
        .groupBy("adverId")
        .agg(
          sum("dailyCost").as("todayCost")
        ).alias("_d")
      d.persist()
      LOG.warn(s"AdverHandler dayCost take(10)", d.take(10))


      val up = a
        .join(t, col("a.ID") === col("_t.adverId"), "left_outer")
        .join(tm, col("a.ID") === col("_tm.adverId"), "left_outer")
        .join(d, col("a.ID") === col("_d.adverId"), "left_outer")
        .selectExpr(
          "a.ID as adverId",
          "nvl(_d.todayCost, 0.0) as todayCost",
          "nvl(_tm.monthCost, 0.0) as monthCost",
          "nvl(_t.totalCost, 0.0) as totalCost"
        )
      up.persist()

      LOG.warn(s"CampaignAdverHandler update data take(10)", up.take(10))

      //凌晨当天数据清零
      var today=dayBDateFormat.format(new Date())
      if(!today.equals(prevDay)) {
        val now = new Date()

        val h = now.getHours
        if(h <= 4) {
          LOG.warn("CampaignAdverHandler today data init")
          var sql =
            s"""
               | update ADVERTISER
               | set
               | TodayCost = 0
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
               | update ADVERTISER
               | set
               | MonthCost = 0.0
              """.stripMargin
          mySqlJDBCClient.execute(sql)
        }
        prevMonth = month
      }

      val ups = up
        .rdd
        .map{x=>
          s"""
             | update ADVERTISER
             | set
             | TodayCost = ROUND(${x.getAs[Double]("todayCost")},4),
             | MonthCost = ROUND(${x.getAs[Double]("monthCost")},4),
             | TotalCost = ROUND(${x.getAs[Double]("totalCost")},4)
             | where ID = ${x.getAs[Int]("adverId")}
           """.stripMargin
        }
        .collect()

      mySqlJDBCClient.executeBatch(ups)

      t.unpersist()
      tm.unpersist()
      d.unpersist()
      up.unpersist()

      LOG.warn(s"CampaignAdverHandler handle done")
    })
  }
}
