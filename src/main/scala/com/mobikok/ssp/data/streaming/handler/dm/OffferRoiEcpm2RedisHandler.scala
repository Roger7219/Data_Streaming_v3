package com.mobikok.ssp.data.streaming.handler.dm

import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, OfferRoiEcpm}
import com.mobikok.ssp.data.streaming.util.JavaMC.Callback
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import io.codis.jodis.{JedisResourcePool, RoundRobinJedisPool}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/12/28.
  */
class OffferRoiEcpm2RedisHandler extends Handler {

  val consumer = "OffferRoiEcpm2RedisHandler_cer"
  val topic = "ssp_report_overall_dwr"//"ssp_report_campaign_dwr"
  val dayDmTable = "ssp_report_overall_dm_day"

  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"
  val KEY_OFFER_ROI = "ssp_offerrio:%s";

  val KEY_EXPIRE: Int = 1 * 24 * 60 * 60


  val KEY_DSP_IMAGE_INFO = "dsp_image:%s"//image info

  var todayDF = DateFormatUtil.CST("yyyy-MM-dd")

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)
  }

  override def doHandle (): Unit = {
    LOG.warn("OffferRoiEcpm2RedisHandler handler starting")

    JavaMC.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new Callback[util.List[HivePartitionPart]] {
      def doCallback(ps: util.List[HivePartitionPart]): java.lang.Boolean ={

        val jedis = jedisPool.getResource

        RunAgainIfError.run({

          ps.foreach{y=>

            LOG.warn("OffferRoiEcpm2RedisHandler handle b_date ", y.value)

            val today =  todayDF.format(new Date())
            val h = new Date().getHours

            //ssp_report_campaign_dm
            if( h >=3 && today.equals(y.value)) {
              //offer  daily roi ,daily ecpm
              val dailyData =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  dm.offerId as id,
                     |  CAST( case sum(dm.cpcBidPrice) + sum(dm.cpmBidPrice)  when 0 then 0.0 else CAST( 10000* ((sum(dm.feeCpcReportPrice ) + sum(dm.feeCpmReportPrice )) - (sum(dm.cpcBidPrice) + sum(dm.cpmBidPrice))) / (sum(dm.cpcBidPrice) + sum(dm.cpmBidPrice))  AS DOUBLE)  END AS BIGINT)/10000.0  AS roi,
                     |  CAST( CASE SUM(dm.showCount)  WHEN 0 THEN 0 ELSE 10000*1000*sum(dm.realRevenue)/cast(sum(dm.showCount)   as DOUBLE) END AS BIGINT)/10000.0 as ecpm
                     |from $dayDmTable dm
                     |where b_date = '${y.value}' and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, offerId
                """.stripMargin
                )
                .collectAsList()
                .map{x=>
                  OfferRoiEcpm(x.getAs[Integer]("id"),x.getAs[Double]("roi"),x.getAs[Double]("ecpm"))
                }

              LOG.warn(s"offer dailyData take 2", dailyData.take(2).mkString("[", ",", "]"))
              LOG.warn(s"offer dailyData take count:", dailyData.count(x => x.!=(null)))
              dailyData.foreach{x=>

                val dailyrkey = String.format(KEY_OFFER_ROI, today)//x.getId)
                val offerInfo = OM.toJOSN(x)

//                LOG.warn(s"offer daily info:",dailyrkey)
//                LOG.warn(s"offer daily info:", offerInfo)

//
                jedis.set(dailyrkey, OM.toJOSN(offerInfo))
                jedis.expire(dailyrkey, KEY_EXPIRE)

              }

            }

          }
        })

        if(jedis != null) {
          jedis.close()
        }
        return true
      }
    }, topic)

    LOG.warn("OffferRoiEcpm2RedisHandler handler done")
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }

}
