package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/8/4.
  */
class CampaignHandler extends Handler{

  private val dayDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-dd")//DateFormatUtil.CST("yyyy-MM-dd")

  var monthDmTable: String = null
  var dayDmTable: String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  val TOADY_NEED_INIT_CER = "CampaignHandler_ToadyNeedInit_cer"
  val TOADY_NEED_INIT_TOPIC = "CampaignHandler_ToadyNeedInit_topic"

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient:GreenplumClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    monthDmTable = "ssp_report_overall_dm_month" //"ssp_report_campaign_month_dm"//handlerConfig.getString("dwr.daily.table")
    dayDmTable = "ssp_report_overall_dm_day" //"`ssp_report_campaign_dm"//handlerConfig.getString("dwr.daily.table")


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

  var prevDay: String = null
  override def handle (): Unit = {
    LOG.warn(s"CampaignHandler handle start")
    RunAgainIfError.run({


      val c = hiveContext
        .read
        .table("CAMPAIGN")
  //      .jdbc(rdbUrl, "CAMPAIGN", rdbProp)
        .select("ID", "ShowCount", "ClickCount", "DailyCost", "TotalCost")
  //      .repartition(1)
        .alias("c")

      LOG.warn("CampaignHandler mysql table CAMPAIGN take(10)", c.take(10))

      val t = hiveContext
        .read
        .table(monthDmTable)
       // .where("(data_type = 'camapgin' or data_type is null) ")
        .groupBy("campaignId")
        .agg(sum("realRevenue").cast("double").as("totalCost"))
        .alias("t")
      LOG.warn(s"CampaignHandler $monthDmTable take(10)", t.take(10))

      val now = CSTTime.now.date

      val up = hiveContext
        .read
        .table(dayDmTable)
        .where(s" (data_type = 'camapgin' or data_type is null) and campaignId is not null and b_date = '${now}' ")
        .groupBy("campaignId")
        .agg(
          sum("showCount").as("showCount"),
          sum("clickCount").as("clickCount"),
          sum("realRevenue").cast("double").as("dailyCost"),
          sum("conversion").as("conversion")
        )
        .alias("d")
        .join(t, col("d.campaignId") === col("t.campaignId"), "left_outer")
        .selectExpr(
          "nvl(d.campaignId, t.campaignId) as campaignId",
          "d.showCount",
          "d.clickCount",
          "d.dailyCost",
          "t.totalCost",
          "d.conversion"
        )
        .alias("n")
        .join(c, col("n.campaignId") === col("c.ID"), "left_outer")
        .selectExpr(
          "nvl(n.campaignId, c.ID) AS campaignId",
          "nvl(n.showCount, 0) AS showCount",
          "nvl(n.clickCount, 0) AS clickCount",
          "nvl(n.dailyCost, 0.0) AS dailyCost",
          "nvl(n.totalCost, 0.0) AS totalCost",
          "nvl(n.conversion, 0) AS conversion"
        )
        .alias("up")
      LOG.warn(s"CampaignHandler update data", "take(10)", up.take(10), "count", up.count())

      // 凌晨数据清零
      MC.pull(TOADY_NEED_INIT_CER, Array(TOADY_NEED_INIT_TOPIC), {x=>
        val toadyNeedInit =  x.isEmpty || ( !x.map(_.getKeyBody).contains(now) )

        if(toadyNeedInit){
          //立即执行，避免后执行会将重置的还原了
          SQLMixUpdater.execute()

          LOG.warn("CampaignHandler today data init")
          //立即执行，避免后执行会将重置的还原了
          SQLMixUpdater.execute()

          var sql =
            s"""
                update CAMPAIGN
               | set
               | ShowCount = 0,
               | ClickCount = 0,
               | DailyCost = 0.0,
               | TodayCaps = 0
               | where status = 1
                """.stripMargin
          mySqlJDBCClient.execute(sql)

        }

        MC.push(new PushReq(TOADY_NEED_INIT_TOPIC, now))
        true
      })

      val ups = up
        .rdd
        .map{x=>
          s"""
             | update CAMPAIGN
             | set
             | ShowCount = ${x.getAs[Long]("showCount")},
             | ClickCount = ${x.getAs[Long]("clickCount")},
             | DailyCost = ROUND(${x.getAs[Double]("dailyCost")}, 4),
             | TotalCost = ROUND(${x.getAs[Double]("totalCost")}, 4),
             | TodayCaps = ${x.getAs[Long]("conversion")}
             | where ID = ${x.getAs[Int]("campaignId")}
           """.stripMargin
        }
        .collect()

        mySqlJDBCClient.executeBatch(ups)

      })

    LOG.warn(s"CampaignHandler handle done")

  }
}
