package com.mobikok.ssp.data.streaming.handler.dm

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{HivePartitionPart, ImageInfo}
import com.mobikok.ssp.data.streaming.handler.dm.Handler
import com.mobikok.ssp.data.streaming.util.MessageClientUtil.Callback
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import io.codis.jodis.RoundRobinJedisPool
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import io.codis.jodis.JedisResourcePool

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/12/28.
  */
class AppImageInfo2RedisHandler extends Handler {

  val consumer = "AppImageInfo2RedisHandler_cer"
  val topic = "ssp_report_overall_dwr" //"ssp_report_campaign_dwr"
  var dayDmTable = "ssp_report_overall_dm_day" //"ssp_report_campaign_dm"
  var monthDmTable = "ssp_report_overall_dm_month" //"ssp_report_campaign_dm"


  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"
  val KEY_APP_SHOW_DAY_INFO = "dsp_show_day:%s" //app日展示key
  val KEY_APP_SHOW_TOTAL_INFO = "dsp_show_total" // app总展示key
  val KEY_APP_BUDGET_DAY_INFO = "dsp_budget_day:%s" //app日限
  val KEY_APP_BUDGET_TOTAL_INFO = "dsp_budget_total" //app总限
  val KEY_EXPIRE: Int = 1 * 24 * 60 * 60


  val KEY_DSP_IMAGE_INFO = "dsp_image:%s"//image info

  var todayDF = DateFormatUtil.CST("yyyy-MM-dd")

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)
  }

  override def handle (): Unit = {
    LOG.warn("AppImageInfo2RedisHandler handler starting")

    MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new Callback[util.List[HivePartitionPart]] {
      def doCallback(ps: util.List[HivePartitionPart]): java.lang.Boolean ={

        val jedis = jedisPool.getResource

        RunAgainIfError.run({

          //今天的
          ps.foreach{y=>

            LOG.warn("AppImageInfo2RedisHandler handle b_date ", y.value)

            val today =  CSTTime.now.date() //todayDF.format(new Date())

            if(today.equals(y.value)) {
              //app 日展示，日金额
              val dailyData =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  appId,
                     |  sum(showCount) as showCount,
                     |  CAST( 100*sum(realRevenue) AS BIGINT)/100000.0 as realRevenue
                     |from $dayDmTable
                     |where b_date = '${y.value}' and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, appId
                """.stripMargin
                )
                .collect()
              //where b_date = '${y.value}' and realRevenue > 0 and (data_type = 'camapgin' or data_type is null)

              LOG.warn("app daily showCount and daily Price take 2", dailyData.take(2))
              LOG.warn("app daily showCount and daily Price", "count", dailyData.count(x => x.!=(null)))
              dailyData.foreach{x=>
                val rkey = String.format(KEY_APP_SHOW_DAY_INFO, today)
                val budgetkey = String.format(KEY_APP_BUDGET_DAY_INFO, today)

//                LOG.warn(s"app daily info to redis rkey", "rkey", rkey,"showCount",x.getAs[Long]("showCount").toDouble , "appId", String.valueOf(x.getAs[Int]("appId")))
//                LOG.warn(s"app daily info to redis budgetkey:","budgetkey", budgetkey, "realRevenue", x.getAs[Double]("realRevenue"),"appId", String.valueOf(x.getAs[Int]("appId")))

                jedis.zadd(rkey, x.getAs[Long]("showCount").toDouble,  String.valueOf(x.getAs[Int]("appId")))
                jedis.expire(rkey, KEY_EXPIRE)

                jedis.zadd(budgetkey, x.getAs[Double]("realRevenue"), String.valueOf(x.getAs[Int]("appId")))
                jedis.expire(budgetkey, KEY_EXPIRE)

              }

              //图片的统计数据
              val info =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  imageId as id,
                     |  sum(clickCount) as todayClick,
                     |  sum(showCount) as todayShowCount,
                     |  CAST( CASE sum(showCount) WHEN 0 THEN 0 ELSE 100000*sum(clickCount)/cast(sum(showCount) as BIGINT) END AS BIGINT)/100000.0 as ctr
                     |from $dayDmTable
                     |where b_date = '${y.value}' and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, imageId
              """.stripMargin)
                  .selectExpr("id", "todayClick", "todayShowCount", "ctr")
                  .collectAsList()
                  .map{x=>
                    ImageInfo(x.getAs[Integer]("id"),x.getAs[Long]("todayClick"),x.getAs[Long]("todayShowCount"),x.getAs[Double]("ctr"))
                  }
              LOG.warn("image daily info take 2",info.toList.take(2).mkString("[", ",", "]"))
              LOG.warn("image daily info count",info.count(x => x.!=(null)))
              info.foreach{x=>
                val rkey = String.format(KEY_DSP_IMAGE_INFO, x.getId)
                val imageInfo = OM.toJOSN(x)
//                LOG.warn(s"image daily info rkey:",rkey)
//                LOG.warn(s"image daily info imageInfo:", imageInfo)

                jedis.set(rkey, OM.toJOSN(imageInfo))
                jedis.expire(rkey, KEY_EXPIRE)
              }
            }
          }

          // 总的
          // app 总展示，总金额（待优化，用月表）
          val totalData =
            sql(
              s"""
                 |select
                 |  b_date,
                 |  appId,
                 |  sum(showCount) as totalShowCount,
                 |  CAST( 100*sum(realRevenue) AS BIGINT)/100000.0 as totalPrice
                 |from $monthDmTable
                 |group by b_date, appId
              """.stripMargin)
              .collect()
          //where realRevenue > 0 and (data_type = 'camapgin' or data_type is null)

          LOG.warn(s"app totalData take 2", totalData.take(2))
          LOG.warn(s"app totalData take count:", totalData.count(x => x.!=(null)))
          totalData.foreach{x=>
            val totalrkey = String.format(KEY_APP_SHOW_TOTAL_INFO)
            val totalBudgetkey = String.format(KEY_APP_BUDGET_TOTAL_INFO)
            //              LOG.warn(s"app total info:", "totalrkey", totalrkey,"totalShowCount", x.getAs[Long]("totalShowCount").toDouble,
            //                "appId", String.valueOf( x.getAs[Integer]("appId")), "totalBudgetkey", totalBudgetkey, "totalPrice", x.getAs[Double]("totalPrice"))

            jedis.zadd(totalrkey, x.getAs[Long]("totalShowCount").toDouble, String.valueOf(x.getAs[Int]("appId")))
            jedis.expire(totalrkey, KEY_EXPIRE)

            jedis.zadd(totalBudgetkey, x.getAs[Double]("totalPrice"), String.valueOf(x.getAs[Int]("appId")))
            jedis.expire(totalBudgetkey, KEY_EXPIRE)
            //              jedis.zincrby(totalrkey, 1, "" + x.getAs[Integer]("appId"))
            //              jedis.zincrby(totalBudgetkey, x.getAs[Double]("totalPrice"), "" + x.getAs[Integer]("appId"))
          }

        })

        if(jedis != null) {
          jedis.close()
        }
        return true
      }
    }, topic)

    LOG.warn("AppImageInfo2RedisHandler handler done")
  }

//  override def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }

}

//object  xv{
//  def main(args: Array[String]): Unit = {
//    val xx = String.format("dsp_show_day:%s", DateFormatUtil.CST("yyyy-MM-dd").format(new Date()))
//    println(xx)
//  }
//}