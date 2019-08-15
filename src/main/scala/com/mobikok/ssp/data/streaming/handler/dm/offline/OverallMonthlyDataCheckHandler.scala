package com.mobikok.ssp.data.streaming.handler.dm.offline

import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{CSTTime, MC, PushReq}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

class OverallMonthlyDataCheckHandler extends Handler{

  val overallDayTable = "ssp_report_overall_dm_day_v2"
  val overallMonthTable = "ssp_report_overall_dm_month"

  val monthValueTopic = "ssp_report_overall_dm_month_topic"
  val dayTabeValueOfMonthTopic = "ssp_report_overall_dm_day_month_topic"


  var monitorClient: MonitorClient = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient ,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName,bigQueryClient, greenplumClient, rDBConfig,kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    monitorClient = new MonitorClient(messageClient)

  }


  override def handle(): Unit = {

    LOG.warn(s"OverallMonthlyDataCheckHandler handle start")

    //每月月初凌晨4点执行一次
    val updateTime = CSTTime.now.modifyHourAsDate(-4)
    MC.pull("overall_month_datacheck_cer", Array("overall_month_datacheck_topic"), { x =>
      val run = CSTTime.now.date().split("-")(2).contains("01") && (x.isEmpty || (!x.map(_.getKeyBody).contains(updateTime)))

      if(run){
//      if(true){

        val dayTabeValueOfMonth = hiveContext
          .sql(s"select sum(realRevenue) as revenue from $overallDayTable where b_date >= date_format(add_months('${CSTTime.now.date()}', -1), 'yyyy-MM-01') and b_date <= date_add(date_format('${CSTTime.now.date()}', 'yyyy-MM-01'), -1) ")
          .collect()
          .map(x =>
            x.getAs[java.math.BigDecimal]("revenue")
          )

        val monthTabeValue = hiveContext
          .sql(s"select sum(realRevenue) as revenue from $overallMonthTable where b_date = date_format(add_months('${CSTTime.now.date()}', -1), 'yyyy-MM-01') ")
          .collect()
          .map(x =>
            x.getAs[java.math.BigDecimal]("revenue")
          )

        //push to messageQueue

        val dateSplit = CSTTime.now.date().split("-")

        val checkedDate = s"${dateSplit(0)}-${dateSplit(1)}-01"

        LOG.warn("OverallMonthlyDataCheckHandler message1", "topic:", dayTabeValueOfMonthTopic, " sum:", dayTabeValueOfMonth.head)

        LOG.warn("OverallMonthlyDataCheckHandler message2", "topic:", monthValueTopic, " sum:", monthTabeValue.head)

        monitorClient.push(new MonitorMessage(dayTabeValueOfMonthTopic, checkedDate, dayTabeValueOfMonth.head.toBigInteger))
        monitorClient.push(new MonitorMessage(monthValueTopic, checkedDate, monthTabeValue.head.toBigInteger))

      }

      MC.push(new PushReq("overall_month_datacheck_topic", updateTime))
      true
    })

    LOG.warn(s"OverallMonthlyDataCheckHandler handle end")
  }

}
