package com.mobikok.ssp.data.streaming.handler.dm

import java.sql.ResultSet

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import io.codis.jodis.{JedisResourcePool, RoundRobinJedisPool}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/12/28.
  */
class OffferCountryStat2RedisHandler extends Handler {

  val consumer = "OffferCountryStat2RedisHandler_cer"
  val topic = "ssp_report_overall_dwr"//"ssp_report_campaign_dwr"
  val dayDmTable = "ssp_report_overall_dm_day"

  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"

  //county 单词错了
  val KEY_OFFER_CLICK_COUNTY = "ssp_click_county:%s:%s"

  val KEY_EXPIRE: Int = 24* 60 * 60 //1 天


  val KEY_DSP_IMAGE_INFO = "dsp_image:%s"//image info

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null


  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build();

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

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
    LOG.warn("OffferCountryStat2RedisHandler handler starting")

    RunAgainIfError.run({

      MC.pullBDateDesc(consumer, Array(topic), {ms=>

        if(ms.length>0) {

          val jedis = jedisPool.getResource

          val endDate = CSTTime.now.date()
          val startDate = CSTTime.now.addHourToDate(-24*8)// 8天

          var clickDF = sql(
            s"""
              |select
              |offerId, countryId, sum(clickCount) as clickCount
              |from $dayDmTable
              |where b_date >= "$startDate" and b_date <= "$endDate"
              |group by offerId, countryId
            """.stripMargin)
            .as("cl")

          LOG.warn("clickDF", "size", clickDF.count(), "take(10)", clickDF.take(10))

          //smartlink 开着的Offer
          var smartlinkOfferDF = mySqlJDBCClient.executeQuery(
            s"""
               |SELECT
               |O.ID AS id,
               |O.Name AS name
               |FROM OFFER O
               |LEFT JOIN CAMPAIGN C ON C.ID = O.CampaignId
               |LEFT JOIN ADVERTISER A ON A.ID =C.AdverId
               |WHERE O.Status=1 AND O.AmStatus=1 AND C.Status=1 AND O.Modes LIKE CONCAT('%,',2,',%') AND C.AdverId!=0
             """.stripMargin, new MySqlJDBCClientV2.Callback[DataFrame] {
              override def onCallback(rs: ResultSet): DataFrame = {
                OM.assembleAsDataFrame(rs, hiveContext)
              }
            })
          .as("sm")

          LOG.warn("smartlinkOfferDF", "size", smartlinkOfferDF.count(), "take(10)", smartlinkOfferDF.take(10))

          var result = smartlinkOfferDF
            .join(clickDF, expr("sm.id")  === expr("cl.offerId"), "inner")
            .select(expr("cl.*"))
            .collectAsList()

          LOG.warn("result", "size", result.size(), "take(10)", result.take(10))

          result.foreach{x=>

            val rkey = String.format(KEY_OFFER_CLICK_COUNTY, x.getAs[Integer]("offerId"), x.getAs[Integer]("countryId"))
            jedis.set(rkey, String.valueOf(x.getAs[java.lang.Long]("clickCount")))
            jedis.expire(rkey, KEY_EXPIRE)
          }

          if(jedis != null) {
            jedis.close()
          }
        }

        true
      })
    })

    LOG.warn("OffferCountryStat2RedisHandler handler done")
  }

}
