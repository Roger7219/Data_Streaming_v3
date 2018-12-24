package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util._
import com.mysql.jdbc.Driver
import com.typesafe.config.Config
import org.apache.spark.sql
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.functions.{expr, sum, _}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/13.
  */
class ImageHandler extends Handler {

  private val simpleDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-dd")//new SimpleDateFormat("yyyy-MM-dd")

  var mySqlJDBCClient: MySqlJDBCClientV2 = null
  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null

  var dwrTable: String = "ssp_report_overall_dwr_day" //"ssp_report_campaign_dwr"
  val TOPIC = "ssp_report_overall_dwr"
  val CONSUMER = "ImageHandler_cer"

  override def init (moduleName: String, bigQueryClient:BigQueryClient, greenplumClient:GreenplumClient,rDBConfig:RDBConfig, kafkaClient: KafkaClient,messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {

    super.init(moduleName, bigQueryClient,greenplumClient, rDBConfig, kafkaClient: KafkaClient,messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

//    dwrTable = handlerConfig.getString("dm.table")

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

    try {

      val d = CSTTime.now.date()
      val h = CSTTime.now.hour() //new Date().getHours

      if (0 <= h && h <= 2) return

      MC.pullBDateDesc(CONSUMER, Array(TOPIC), {x=>

        //是否包含当天的b_date
        if(x.filter(y=>d.equals(y.value)).isEmpty) {
          return true
        }

        // IMAGE_PERCENT表中只有一条配置记录
        val ip = hiveContext
          .read
          .jdbc(rdbUrl, "(select * from IMAGE_PERCENT limit 1) as t0", rdbProp)
          .repartition(1)
          .alias("ip")
        ip.createOrReplaceTempView("ip")

        RunAgainIfError.run{
          val ipJoined = sql(
            s"""
               | select
               |   st.imageId,
               |   st.todayShowCount ,
               |   st.todayClickCount,
               |   cast(ip.Ctr as double) as ctr
               | from (
               |
               |   select
               |     imageId,
               |     sum(clickCount) as todayClickCount,
               |     sum(showCount)  as todayShowCount
               |   from $dwrTable
               |   where b_date = '$d' and imageId > 0 and appId <> 814 and appId <> 42 and appId <> 72
               |   group by imageId
               | ) st
               | cross join ip
        """.stripMargin)

        LOG.warn("ImageHandler ipJoined take(10)", ipJoined.take(10))

        val s0 = ListBuffer[String]()

        ipJoined
          .collect()
          .foreach { z =>

            val cc = z.getAs[Long]("todayClickCount")
            val sc = z.getAs[Long]("todayShowCount")
            val ctr = z.getAs[Double]("ctr")
            s0 += s"""
                     | update IMAGE_INFO
                     | set
                     | todayClickCount = ${cc},
                     | todayShowCount = ${sc},
                     | status = if( ShowCount > 0 and ShowCount < ${sc} and $cc/$sc < $ctr, 0, status)
                     | where id =  ${z.getAs[Int]("imageId")}
                 """.stripMargin
          }

          mySqlJDBCClient.executeBatch(s0.toArray[String], 500)
        }


        true
      })

      LOG.warn("ImageHandler handle done!")

    } catch {
      case e: Exception => {
        throw new HandlerException(classOf[ImageHandler].getSimpleName + " Handle Fail：", e)
      }
    }

  }

}
