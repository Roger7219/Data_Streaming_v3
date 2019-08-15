package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util.{HashMap, List, Map}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.Resp
import com.mobikok.message.client.MessageClient
import com.mobikok.monitor.client.{MonitorClient, MonitorMessage}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._


/**
  * Created by Administrator on 2018/8/7 0007.
  */
class BigQueryOverallDailyDataCheckHandler extends  Handler{
  val dayTopic = "bigquery_overall_day_check_topic"
  val hourTopic = "bigquery_overall_hour_check_topic"

  var monitorClient: MonitorClient = null

  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    monitorClient = new MonitorClient(messageClient)
  }


  override def handle(): Unit = {


    LOG.warn(s"BigQueryOverallDailyDataCheckHandler handle start")

    val checkedDate = CSTTime.now.modifyHourAsDate(-24)

    var dayRealRevenue :Double = 0.0
    var dayRealConversion :Int = 0

    val hourRealRevenues = new ListBuffer[Double]
    val hourRealConversion = new ListBuffer[Int]

    //每天凌晨5点执行一次
    val updateTime = CSTTime.now.modifyHourAsDate(-5)
    MC.pull("bigquery_overall_dailydatacheck_cer", Array("bigquery_overall_dailydatacheck_topic"), { x =>
      val run = x.isEmpty || (!x.map(_.getKeyBody).contains(updateTime))

      if(run){

//      if(true){
        val startDate = checkedDate
        val endDate = checkedDate

        val dayValueUrl = s"http://104.250.136.138:5555/SightOverallReport/v2?roleId=0&roleType=SP&endDate=$startDate&reportType=admin&startDate=$endDate&groupBy=statDate&length=31"

        val hourValueUrl = s"http://104.250.136.138:5555/SightOverallReport/v2?roleId=0&roleType=SP&endDate=$startDate&reportType=admin&startDate=$endDate&groupBy=statDate,statTime&orderBy=statTime&length=31"//s"http://104.250.136.138:5555/SightOverallReport?groupBy=statDate,statTime&length=2000000&startDate=$startDate&endDate=$endDate"

        val dayResponseBody = HttpUtils.sendGet(dayValueUrl, new HashMap[String, String])
        val hourResponseBody = HttpUtils.sendGet(hourValueUrl, new HashMap[String, String])


        val dayRespData = OM.toBean(dayResponseBody, new TypeReference[Resp[Object]]() {})
        val hourRespData = OM.toBean(hourResponseBody, new TypeReference[Resp[Object]]() {})

        val dayList = dayRespData.getPageData.asInstanceOf[List[Map[String, Any]]]
        val hourList = hourRespData.getPageData.asInstanceOf[List[Map[String, Any]]]

        for(m <- dayList){
          dayRealRevenue = m.get("realRevenue").asInstanceOf[Double]
          dayRealConversion = m.get("realConversion").asInstanceOf[Int]
        }

        for(m <- hourList){
          hourRealRevenues += m.get("realRevenue").asInstanceOf[Double]
          hourRealConversion  += m.get("realConversion").asInstanceOf[Int]
        }

        LOG.warn("BigQueryOverallDailyDataCheckHandler", "checkedDate", checkedDate, "dayRealRevenue", dayRealRevenue, "dayRealConversion",
          dayRealConversion, "hourRealRevenues", hourRealRevenues.sum, "hourRealConversion", hourRealConversion.sum)

        monitorClient.push(new MonitorMessage(dayTopic, checkedDate, dayRealRevenue.round))
        monitorClient.push(new MonitorMessage(hourTopic, checkedDate, hourRealRevenues.sum.round))

      }

      MC.push(new PushReq("bigquery_overall_dailydatacheck_topic", updateTime))
      true
    })


    LOG.warn(s"BigQueryOverallDailyDataCheckHandler handle end")
  }

}
