package com.mobikok.ssp.data.streaming.handler.dm

import java.io.File
import java.net.InetAddress
import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Date}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{BigQueryJDBCClient, MessageClientUtil, RunAgainIfError}
import com.mobikok.ssp.data.streaming.util.MessageClientUtil.Callback
import com.typesafe.config.Config
import io.codis.jodis.RoundRobinJedisPool
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
  * Created by admin on 2017/9/4.
  */
@deprecated
class RedisDayMonthLimitHandler extends Handler {

  import io.codis.jodis.JedisResourcePool


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

  var todayDF = new SimpleDateFormat("yyyy-MM-dd")

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)
  }

  var bigQueryJDBCClient = new BigQueryJDBCClient("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath=/usr/bigquery/key.json;Timeout=3600;")

  override def handle (): Unit = {
    LOG.warn("RedisDayMonthLimitHandler handler starting")

    MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new Callback[util.ArrayList[HivePartitionPart]] {
      def doCallback(ps: util.ArrayList[HivePartitionPart]): java.lang.Boolean ={


        RunAgainIfError.run({
          var jedis: Jedis = null
          val hc = hiveContext
          import hc.implicits._

          try{
            jedis = jedisPool.getResource

            //take(2) for test !!
            ps.take(2).foreach{y=>

              var b_date = y.value
              LOG.warn("RedisDayMonthLimitHandler handle b_date ", b_date)

              var today =  todayDF.format(new Date());
              var data: util.List[util.Map[String, Object]] = null

              if(today.equals(b_date)) {
                //CPA日月限
                data = bigQueryJDBCClient.executeQuery(
                    s"""
                       |select
                       | b_date,
                       | campaignId,
                       | CAST( 100000*sum(feeReportPrice) AS INT64)/100000.0  as feeReportPrice
                       |from report_dataset.ssp_report_campaign_dm
                       |where _PARTITIONTIME = "${b_date}" and b_date = "${b_date}" and feeReportPrice > 0 and (data_type = 'camapgin' or data_type is null)
                       |
                       |group by b_date, campaignId
                    """.stripMargin,
                    classOf[java.util.Map[String,Object]]);
                LOG.warn("CPA day-month limit", data.take(5).asJava )
                data.foreach{x=>
                  jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", x.get("feeReportPrice").asInstanceOf[Double], x.get("campaignId").toString )
                  jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", KEY_EXPIRE)
                }
              }

              //CPA campaign总的budget
              data = bigQueryJDBCClient.executeQuery(
                  s"""
                     |select
                     |  campaignId,
                     |  CAST( 100000*sum(feeReportPrice) AS INT64)/100000.0  as feeReportPrice
                     |from report_dataset.ssp_report_campaign_dm
                     |where feeReportPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by campaignId
                  """.stripMargin,
                  classOf[java.util.Map[String,Object]])
              LOG.warn("CPA day-month campaign budget", data.take(5).asJava)
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO", x.get("feeReportPrice").asInstanceOf[Double], x.get("campaignId").asInstanceOf[Long].toString )
              }

              //CPM日月限 展示
              if(today.equals(y.value)) {
                data = bigQueryJDBCClient.executeQuery(
                  s"""
                     |select
                     |  b_date,
                     |  campaignId,
                     |  CAST( 100000*sum(cpmBidPrice) AS INT64)/100000.0  as cpmBidPrice
                     |from report_dataset.ssp_report_campaign_dm
                     |where _PARTITIONTIME = "${b_date}" and b_date = "${b_date}" and cpmBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, campaignId
                    """.stripMargin,
                  classOf[java.util.Map[String,Object]])
                LOG.warn("CPM day-month limit", data.take(5).asJava)
                data.foreach{x=>
                  jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", x.get("cpmBidPrice").asInstanceOf[Double], x.get("campaignId").asInstanceOf[Long].toString)
                  jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", KEY_EXPIRE)
                }
              }

              //CPM campaign总的budget
              data = bigQueryJDBCClient.executeQuery(
                s"""
                   |select
                   |  campaignId,
                   |  CAST( 100000*sum(cpmBidPrice) AS INT64)/100000.0  as cpmBidPrice
                   |from report_dataset.ssp_report_campaign_dm
                   |where cpmBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                   |group by campaignId
                  """.stripMargin,
                classOf[java.util.Map[String,Object]])
              LOG.warn("CPM day-month campaign budget", data.take(5).asJava)
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO", x.get("cpmBidPrice").asInstanceOf[Double], x.get("campaignId").asInstanceOf[Long].toString)
              }

              //CPC日月限 点击
              if(today.equals(y.value)) {
                data = bigQueryJDBCClient.executeQuery(
                    s"""
                       |select
                       |  b_date,
                       |  campaignId,
                       |  CAST( 100000*sum(cpcBidPrice) AS INT64)/100000.0  as cpcBidPrice
                       |from report_dataset.ssp_report_campaign_dm
                       |where _PARTITIONTIME = "${b_date}" and b_date = "${b_date}" and cpcBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                       |group by b_date, campaignId
                    """.stripMargin,
                    classOf[java.util.Map[String,Object]])
                LOG.warn("CPC day-month limit", data.take(5).asJava)
                data.foreach{x=>
                  jedis.zadd(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", x.get("cpcBidPrice").asInstanceOf[Double], x.get("campaignId").asInstanceOf[Long].toString)
                  jedis.expire(s"$KEY_CAMPAIGN_DAY_INFO:$b_date", KEY_EXPIRE)
                }
              }

              //CPC campaign总的budget
              data = bigQueryJDBCClient.executeQuery(
                  s"""
                     |select
                     |  b_date,
                     |  campaignId,
                     |  CAST( 100000*sum(cpcBidPrice) AS INT64)/100000.0  as cpcBidPrice
                     |from report_dataset.ssp_report_campaign_dm
                     |where cpcBidPrice > 0 and (data_type = 'camapgin' or data_type is null)
                     |group by b_date, campaignId
                  """.stripMargin,
                  classOf[java.util.Map[String,Object]])
              LOG.warn("CPC day-month campaign budget", data.take(5).asJava)
              data.foreach{x=>
                jedis.zadd(s"$KEY_CAMPAIGN_TOTAL_INFO:$b_date", x.get("cpcBidPrice").asInstanceOf[Double], x.get("campaignId").asInstanceOf[Long].toString)
              }

            }

          }
          catch {case e:Throwable=>
            throw new RuntimeException(e)
          }
          finally {
            if(jedis != null) {
              jedis.close()
            }
          }
        })
        return true
      }
    }, topic);

    LOG.warn("RedisUpdateHandler handler done")
  }




}


