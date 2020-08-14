package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClientApi
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.beans.BeanProperty
/**
  * Created by Administrator on 2017/9/2.
  */
class IntegrationModeCapsHandler extends  Handler{

  private val CONSUMER = "IntegrationModeCapHandler_consumer"
  private val TOPICS = Array("ssp_report_overall_dwr")

  val TOADY_NEED_INIT_CER = "IntegrationModeCapHandler_ToadyNeedInit_cer"
  val TOADY_NEED_INIT_TOPIC = "IntegrationModeCapHandler_ToadyNeedInit_topic"

  var mySqlJDBCClient: MySqlJDBCClient = null

  override def init (moduleName: String, bigQueryClient:BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

    mySqlJDBCClient = new MySqlJDBCClient(
      moduleName,
      handlerConfig.getString(s"rdb.url"),
      handlerConfig.getString(s"rdb.user"),
      handlerConfig.getString(s"rdb.password")
    )
    SQLMixUpdater.init(mySqlJDBCClient)

  }

  override def doHandle (): Unit = {
  
    val now = CSTTime.now.date

    // 凌晨数据清零
    messageClient.pull(TOADY_NEED_INIT_CER, Array(TOADY_NEED_INIT_TOPIC), { x=>
      val toadyNeedInit =  x.isEmpty || ( !x.map(_.getKeyBody).contains(now) )

      if(toadyNeedInit){
        //立即执行，避免后执行会将重置的还原了
        SQLMixUpdater.execute()

        mySqlJDBCClient.execute(s""" update OFFER set TodayModeCaps = "" WHERE status=1 and amStatus=1 """)
      }
      messageClient.push(new PushReq(TOADY_NEED_INIT_TOPIC, now))
      true
    })

    //准实时刷新caps
    messageClient.pullBDateDesc(
      CONSUMER,
      TOPICS,
      {x=>
        x.map(x=>x.value).filter(_.equals(now)).foreach{b_date=>

          var d: Array[Row] = null
          RunAgainIfError.run{
            d = sql(
              s"""
                 | select
                 |   offerId,
                 |   CAST( sum(if(appModeId = 1, conversion, 0)) AS BIGINT ) as sdkCaps,
                 |   CAST( sum(if(appModeId = 2, conversion, 0)) AS BIGINT ) as smartCaps,
                 |   CAST( sum(if(appModeId = 3, conversion, 0)) AS BIGINT ) as onlineCaps,
                 |   CAST( sum(if(appModeId = 4, conversion, 0)) AS BIGINT ) as jsCaps,
                 |   CAST( sum(if(appModeId = 5, conversion, 0)) AS BIGINT ) as rtbCaps,
                 |   CAST( sum(if(appModeId = 6, conversion, 0)) AS BIGINT ) as cpaCaps,
                 |   CAST( sum(if(appModeId = 8, conversion, 0)) AS BIGINT ) as cpiCaps
                 | from ssp_report_overall_dm
                 | where (data_type = 'camapgin' or data_type is null) and b_date = "${b_date}"
                 | group by offerId
            """.stripMargin
            )
            .rdd
            .collect()
          }

          LOG.warn(s"IntegrationModeCapsHandler update data start", "b_date", b_date, "SQLMixUpdater count", SQLMixUpdater.count())
          d.filter{x=>
            //过滤掉全部是0值的
            (x.getAs[Long]("sdkCaps") != 0
            || x.getAs[Long]("smartCaps") != 0
            || x.getAs[Long]("onlineCaps") != 0
            || x.getAs[Long]("jsCaps") != 0
            || x.getAs[Long]("rtbCaps") != 0
            || x.getAs[Long]("cpaCaps") != 0
            || x.getAs[Long]("cpiCaps") != 0
            )
          }
          .map{x=>

            var caps = OM.toJOSN(Caps(
              x.getAs[Long]("sdkCaps"),
              x.getAs[Long]("smartCaps"),
              x.getAs[Long]("onlineCaps"),
              x.getAs[Long]("jsCaps"),
              x.getAs[Long]("rtbCaps"),
              x.getAs[Long]("cpaCaps"),
              x.getAs[Long]("cpiCaps")
            ), false).replaceAll("\"", "\\\\\"")

            SQLMixUpdater.addUpdate("OFFER", "ID", x.getAs[Int]("offerId"), "TodayModeCaps", caps)
            //待删
            s"""
               | update OFFER
               | set TodayModeCaps = "$caps"
               | where id = ${x.getAs[Int]("offerId")}
               """.stripMargin
          }

          LOG.warn(s"IntegrationModeCapsHandler update data done", "b_date", b_date, "SQLMixUpdater count", SQLMixUpdater.count())

//          mySqlJDBCClient.executeBatch(d, 500)

        }

        true
      }
    )

  }
}
//
//object x24{
//
//  def main (args: Array[String]): Unit = {
//    println(""" {"}" """.replaceAll("\"", "\\\\\""))
//  }
//}

case class Caps(@BeanProperty sdkCaps:Long = 0,
                @BeanProperty smartCaps: Long = 0,
                @BeanProperty onlineCaps:Long = 0,
                @BeanProperty jsCaps: Long= 0,
                @BeanProperty rtbCaps:Long= 0,
                @BeanProperty cpaCaps:Long= 0,
                @BeanProperty cpiCaps:Long = 0) {

}