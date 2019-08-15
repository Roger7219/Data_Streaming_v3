package com.mobikok.ssp.data.streaming.handler.dm.offline

import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util.{CSTTime, MC, PushReq}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2018/7/26 0026.
  */
class OverallDataCheckHandler extends Handler{

  val overallHourTable = "ssp_report_overall_dm"
  val overallDayTable = "ssp_report_overall_dm_day_v2"

  val dayTopic = "ssp_report_overall_dm_day_v2_topic"
  val hourTopic = "ssp_report_overall_dm_topic"


  var monitorClient: MonitorClient = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient ,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName,bigQueryClient, greenplumClient, rDBConfig,kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    monitorClient = new MonitorClient(messageClient)

  }


  override def handle(): Unit = {

    LOG.warn(s"OverallDataCheckHandler handle start")

    val checkedDate = CSTTime.now.modifyHourAsDate(-24)

    //每天凌晨4点执行一次
    val updateTime = CSTTime.now.modifyHourAsDate(-4)
    MC.pull("overall_datacheck_cer", Array("overall_datacheck_topic"), { x =>
      val run = x.isEmpty || (!x.map(_.getKeyBody).contains(updateTime))

      if(run){

        val dayValue = hiveContext
          .sql(s"select sum(realRevenue) as revenue from $overallDayTable where b_date = '$checkedDate' ")
          .collect()
          .map(x =>
            x.getAs[java.math.BigDecimal]("revenue")
          )

        val hourValue = hiveContext
          .sql(s"select sum(realRevenue) as revenue from $overallHourTable where b_date = '$checkedDate' ")
          .collect()
          .map(x =>
             x.getAs[java.math.BigDecimal]("revenue")
          )


        //push to messageQueue


        LOG.warn("OverallDataCheckHandler message1", "topic:", dayTopic, " date:", checkedDate, " sum:", dayValue.head)

        LOG.warn("OverallDataCheckHandler message2", "topic:", hourTopic, " date:", checkedDate, " sum:", hourValue.head)

        monitorClient.push(new MonitorMessage(dayTopic, checkedDate, dayValue.head))
        monitorClient.push(new MonitorMessage(hourTopic, checkedDate, hourValue.head))

      }

      MC.push(new PushReq("overall_datacheck_topic", updateTime))
      true
    })

    LOG.warn(s"OverallDataCheckHandler handle end")
  }

}
