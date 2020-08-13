package com.mobikok.ssp.data.streaming.handler.dm

import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.JavaMC.Callback
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import io.codis.jodis.{JedisResourcePool, RoundRobinJedisPool}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2018/05/30.
  */
class Clicks2RedisHandler extends Handler {

  val consumer = "Clicks2RedisHandler_cer"
  val topic = "ssp_report_overall_dwr"
  val DmTable = "ssp_report_overall_dm"

  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"
  val KEY_CLICK_HOUR = "ssp_click_hour:%s:%s"

  val KEY_EXPIRE: Int = 1 * 24 * 60 * 60

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient, messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)
  }

  override def doHandle (): Unit = {
    LOG.warn("Clicks2RedisHandler handler starting")

    JavaMC.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new Callback[util.List[HivePartitionPart]] {
      def doCallback(ps: util.List[HivePartitionPart]): java.lang.Boolean ={

        val jedis = jedisPool.getResource

        RunAgainIfError.run({

          ps.foreach{y=>

            LOG.warn("Clicks2RedisHandler handle b_date ", y.value)

            val now = new Date()

            var minutes = now.getMinutes
            val lastTime = CSTTime.now.addHourToBTime(-1)


            LOG.warn("Clicks2RedisHandler handle ", "now: " , now, "minutes" , minutes, "lastTime", lastTime)

            if( minutes >= 30  && minutes <= 36 && y.value.equals(CSTTime.now.date())) {

              val clicksDate =
                sql(
                  s"""
                     |SELECT
                     |  countryid,
                     |  carrierid,
                     |  sum(clickcount)  as clickcount
                     |FROM $DmTable dm
                     |WHERE
                     |  appModeId = 2
                     |  and b_time = '$lastTime'
                     |GROUP BY
                     |  countryid,carrierid
                """.stripMargin
                )
                .collect

              clicksDate.foreach{x=>

                val clicksKey = String.format(KEY_CLICK_HOUR, String.valueOf(x.getAs[Int]("countryid")), String.valueOf(x.getAs[Int]("carrierid")))

//                LOG.warn("OffferRoiEcpm2RedisHandler handle ", "clicksKey", clicksKey)
//                LOG.warn("clicksDate take 2", clicksDate.take(2).mkString("[", ",", "]"))
//                LOG.warn("clicksDate take count:", clicksDate.count(x => x.!=(null)))

                jedis.set(clicksKey, String.valueOf(x.getAs[java.math.BigInteger]("clickcount")))
                jedis.expire(KEY_CLICK_HOUR, KEY_EXPIRE)

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

    LOG.warn("Clicks2RedisHandler handler done")
  }

}
