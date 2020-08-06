package com.mobikok.ssp.data.streaming.handler.dm

import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.MessageClientUtil.Callback
import com.mobikok.ssp.data.streaming.util.{DateFormatUtil, MessageClientUtil, RunAgainIfError}
import com.typesafe.config.Config
import io.codis.jodis.RoundRobinJedisPool
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
/**
  * Created by admin on 2017/9/4.
  */
class RedisDayMonthLimitHandlerV2 extends Handler {

  import io.codis.jodis.JedisResourcePool


  var dayDmTable = "ssp_report_overall_dm_day" // "ssp_report_campaign_dm"
  var monthDmTable = "ssp_report_overall_dm_month" // "ssp_report_campaign_month_dm"
  val consumer = "RedisDayMonthLimitHandler_cer"
  val topic = "ssp_report_campaign_dwr"

  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"
  val KEY_OFFER_INFO_BIDPRICE = "ssp_offerbidprice:%s:%s:%s"
  val KEY_OFFER_INFO_SDK_BIDPRICE = "ssp_sdkbid:%s:%s:%s"
  val KEY_OFFER_INFO_SMART_BIDPRICE = "ssp_smartbid:%s:%s:%s"

  val KEY_EXPIRE: Int = 1 * 24 * 60 * 60
  val KEY_CAMPAIGN_DAY_INFO = "ssp_campaigndayinfo" //campaign放量信息
  val KEY_CAMPAIGN_TOTAL_INFO = "ssp_campaigntotalinfo"

  var todayDF = DateFormatUtil.CST("yyyy-MM-dd")

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)
  }

//  var bigQueryJDBCClient = new BigQueryJDBCClient("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath=/root/kairenlo/data-rest/key.json;Timeout=3600;")

  override def handle (): Unit = {
    LOG.warn("RedisDayMonthLimitHandler handler starting")

    MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new Callback[util.List[HivePartitionPart]] {
      def doCallback(ps: util.List[HivePartitionPart]): java.lang.Boolean ={

        var jedis = jedisPool.getResource

        RunAgainIfError.run({

          ps.foreach{y=>

            var b_date = y.value
            LOG.warn("RedisDayMonthLimitHandler handle b_date ", y.value)

            var today =  todayDF.format(new Date());
            var data: Array[Row] = null

            if(today.equals(b_date)) {
              //CPA日月限
              data =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  campaignId,
                     |  CAST( CAST( 100000*sum(feeReportPrice) AS BIGINT)/100000.0 AS DOUBLE) as feeReportPrice
                     |from $dayDmTable
                     |where b_date = '${b_date}' and feeReportPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, campaignId
                """.stripMargin
                )
                .collect();
              LOG.warn("CPA day-month limit", data.take(5))
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", x.getAs[Double]("feeReportPrice"), String.valueOf(x.getAs[Int]("campaignId")))
                jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", KEY_EXPIRE)
              }
            }

            //CPM日月限 展示
            if(today.equals(b_date)) {
              data =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  campaignId,
                     |  CAST( CAST( 100000*sum(cpmBidPrice) AS BIGINT)/100000.0 AS DOUBLE) as cpmBidPrice
                     |from $dayDmTable
                     |where b_date = '${b_date}' and cpmBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, campaignId
               """.stripMargin)
                .collect()
              LOG.warn("CPM day-month limit", data.take(5))
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", x.getAs[Double]("cpmBidPrice"), String.valueOf(x.getAs[Int]("campaignId")))
                jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", KEY_EXPIRE)
              }
            }

            //CPC日月限 点击
            if(today.equals(y.value)) {
              data =
                sql(
                  s"""
                     |select
                     |  b_date,
                     |  campaignId,
                     |  CAST( CAST( 100000*sum(cpcBidPrice) AS BIGINT)/100000.0 AS DOUBLE) as cpcBidPrice
                     |from $dayDmTable
                     |where b_date = '${b_date}' and cpcBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, campaignId
               """.stripMargin)
                .collect()
              LOG.warn("CPC day-month limit", data.take(5))
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:${b_date}", x.getAs[Double]("cpcBidPrice"), String.valueOf(x.getAs[Int]("campaignId")))
                jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:${b_date}", KEY_EXPIRE)
              }
            }
          }

          var data: Array[Row] = null
          //CPA campaign总的budget
          data =
            sql(
              s"""
                 |select
                 |  campaignId,
                 |  CAST( CAST( 100000*sum(feeReportPrice) AS BIGINT)/100000.0 AS DOUBLE)  as feeReportPrice
                 |from $monthDmTable
                 |where feeReportPrice > 0
                 |group by campaignId
              """.stripMargin
            )
              .collect()
          LOG.warn("CPA day-month campaign budget", data.take(5))
          data.foreach{x=>
            jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO", x.getAs[Double]("feeReportPrice"), String.valueOf(x.getAs[Int]("campaignId")))
          }

          //CPM campaign总的budget
          data =
            sql(
              s"""
                 |select
                 |  campaignId,
                 |  CAST( CAST( 100000*sum(cpmBidPrice) AS BIGINT)/100000.0 AS DOUBLE) as cpmBidPrice
                 |from $monthDmTable
                 |where cpmBidPrice > 0
                 |group by campaignId
              """.stripMargin)
              .collect()
          LOG.warn("CPM day-month campaign budget", data.take(5))
          data.foreach{x=>
            jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO", x.getAs[Double]("cpmBidPrice"), String.valueOf(x.getAs[Int]("campaignId")))
          }

          //CPC campaign总的budget
          data =
            sql(
              s"""
                 |select
                 |  b_date,
                 |  campaignId,
                 |  CAST( CAST( 100000*sum(cpcBidPrice) AS BIGINT)/100000.0 AS DOUBLE) as cpcBidPrice
                 |from $monthDmTable
                 |where cpcBidPrice > 0
                 |group by b_date, campaignId
              """.stripMargin)
              .collect()
          LOG.warn("CPC day-month campaign budget", data.take(5))
          data.foreach{x=>
            jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO", x.getAs[Double]("cpcBidPrice"), String.valueOf(x.getAs[Int]("campaignId")))
          }



        })

        if(jedis != null) {
          jedis.close()
        }
        return true
      }
    }, topic);

    LOG.warn("RedisUpdateHandler handler done")
  }

//  def sql(sqlText: String): DataFrame ={
//    LOG.warn("Execute HQL", sqlText)
//    hiveContext.sql(sqlText)
//  }

}


