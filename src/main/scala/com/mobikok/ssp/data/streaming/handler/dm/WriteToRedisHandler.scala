package com.mobikok.ssp.data.streaming.handler.dm

import java.util

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.{BidPriceList, CountryCarrierConfig, HivePartitionPart}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import io.codis.jodis.RoundRobinJedisPool
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._


/**
  * Created by Administrator on 2017/12/11.
  */
class WriteToRedisHandler extends Handler{

  var jedisPool = null.asInstanceOf[RoundRobinJedisPool]
  var jedis = null.asInstanceOf[Jedis]

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null
  var rdbTable: String = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  var consumer = "writeToRedisCer"
  var dmTable : String = null
  var topics: Array[String] = null


  override def init (moduleName: String, bigQueryClient:BigQueryClient,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient:MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig,handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient: KafkaClient,messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    dmTable = handlerConfig.getString("table")
    topics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))

    rdbUrl = handlerConfig.getString(s"rdb.url")
    rdbUser = handlerConfig.getString(s"rdb.user")
    rdbPassword = handlerConfig.getString(s"rdb.password")
    rdbTable = handlerConfig.getString(s"rdb.table")

    rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }

    mySqlJDBCClient = new MySqlJDBCClientV2(
      moduleName, rdbUrl, rdbUser, rdbPassword
    )

    jedisPool = WriteToRedisHandler.initRedis()
  }

  override def handle (): Unit = {

    // 延时问题
    LOG.warn(s"CountryCarrierInfoToRedisHandler read mysql table $rdbTable starting")
    RunAgainIfError.run({

      //write country and carrier info to Redis from mysql
      val rdbt = s"(select countryId, carrierId, percent from ${rdbTable} ) as t0"
      val bidList = hiveContext
        .read
        .jdbc(rdbUrl, rdbt, rdbProp)
//        .select("countryId", "carrierId", "percent")
        .collectAsList()
        .map{x=>
          CountryCarrierConfig(x.getAs[Integer]("countryId"), x.getAs[Integer]("carrierId"), x.getAs[Integer]("percent"))
        }
        .toList

  //    LOG.warn(s"bidList info count: ${bidList.count(x => x.!=(null))}, take",OM.toJOSN(bidList))
      val xx = bidList.map{x => s"(countryId: ${x.countryId}, carrierId: ${x.carrierId}, percent: ${x.percent})"}.take(4).mkString("[", "," , "]")

      LOG.warn(s"bidList info count: ${bidList.count(x => x.!=(null))}, take(4) : $xx")


      writeToRedis[CountryCarrierConfig](bidList, info=>String.format(Constants.KEY_COUNTRY_CONFIG, info.getCountryId, info.getCarrierId) )

  //write offer BidPrice info
      MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.List[HivePartitionPart]]{

        def doCallback (ps: util.List[HivePartitionPart]):java.lang.Boolean={

          ps.foreach{x=>

            LOG.warn("where b_date", x.value)

            //write bidPriceList to redis from hive table ssp_report_campaign_dm

            val BidPriceList1 = getBidPriceList(x.value, dmTable, "camapgin", 0)
            val BidPriceList2 = getBidPriceList(x.value, dmTable, "overall", 1)
            val BidPriceList3 = getBidPriceList(x.value, dmTable, "overall", 2)

            writeToRedis[BidPriceList](BidPriceList1,
              info=>String.format(Constants.KEY_OFFER_INFO_BIDPRICE, info.getId, info.getCountryId, info.getCarrierId))

            //write sdkBidPriceList to redis from hive table ssp_report_campaign_dm
            writeToRedis[BidPriceList](BidPriceList2,
              info=>String.format(Constants.KEY_OFFER_INFO_SDK_BIDPRICE, info.getId, info.getCountryId, info.getCarrierId))

            //write smartBidPriceList to redis from hive table ssp_report_campaign_dm
            writeToRedis[BidPriceList](BidPriceList3,
              info=>String.format(Constants.KEY_OFFER_INFO_SMART_BIDPRICE, info.getId, info.getCountryId, info.getCarrierId))

          }
          return true

        }
      }, topics:_*)
    })

    LOG.warn(s"CountryCarrierInfoToRedisHandler handle done")

  }


  def writeToRedis[T](bidList: => List[T]/*bidList:List[Object]*/,/* keyType:String,*/ rkey: T => String): Unit = {

    if (bidList != null && bidList.size > 0) {
      var jedis:Jedis = null
      try {
        jedis = jedisPool.getResource

        for (info <- bidList) {

          var k = rkey(info)

          jedis.set(k, OM.toJOSN(info))
          jedis.expire(k, 24 * 60 * 60 + Math.round(Math.random() * 100).toInt) //Math.random.toInt * 100)

//          LOG.warn(s"set ok , key: $k ",OM.toJOSN(info))
        }
      } catch {
        case e: Exception =>
          LOG.warn(e.getMessage)
      }finally {
        if(jedis != null) {
          jedis.close()
        }
      }
    }
  }

  def getBidPriceList(statDate :String, table :String, dataType :String, mode : Int): List[BidPriceList] = {

    val con = if( 0 == mode )  "" else s"and A.mode = $mode"
    val join = if(0 == mode)   "" else s"LEFT JOIN APP A ON A.Id = D.AppId"
    val wheres = s"D.b_date = '$statDate' and ( D.data_type = '${dataType}' or D.data_type is null ) "  +  con

    val appDf = hiveContext
      .read
      .table("APP")
//      .jdbc(rdbUrl, "APP", rdbProp)
      .select("id","mode")

    LOG.warn(s"appDf count: ${appDf.count()} info :",appDf.take(2))

    appDf.registerTempTable("APP")

    val list = hiveContext
      .sql(
      s"""
         |SELECT t.*
         |FROM
         |  (SELECT D.OfferId   as id,
         |       D.CarrierId as carrierId,
         |       D.countryId as countryId,
         |       ROUND(COALESCE(CASE SUM(D.clickCount) WHEN 0 THEN 0.0000 ELSE SUM(D.realRevenue)/SUM(D.clickCount) END,0.0001),4) as bidPrice
         |  FROM $table D
         |  $join
         |  WHERE $wheres
         |  GROUP BY D.b_date,D.offerId,D.countryId,D.carrierId
         |  )t
         |WHERE t.bidPrice > 0
         |
       """.stripMargin)
      .collectAsList()
      .map{x =>
        new BidPriceList(x.getAs[Integer]("id"), x.getAs[Integer]("carrierId"), x.getAs[Integer]("countryId"), x.getAs[java.math.BigDecimal]("bidPrice"))
      }

    val xx = list.map{x => s"(id: ${x.id}, carrierId: ${x.carrierId}, countryId: ${x.countryId}, bidPrice: ${x.bidPrice})"}.take(4).mkString("[", "," , "]")

    LOG.warn(s"getBidPriceList", "count", list.count(x => x.!=(null)), "data_type", dataType, "info", xx)

    list.toList
  }

}

object WriteToRedisHandler{
  def initRedis(): RoundRobinJedisPool ={
    val jedisPool = RoundRobinJedisPool.create().curatorClient(Constants.HOST_PORT, 30000)
      .zkProxyDir(
        Constants.ZK_PROXY_DIR).build()
    jedisPool
  }
}

object Constants{
  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"
//  val HOST_PORT = "192.168.1.244:2181,192.168.1.246:2181"
  val KEY_OFFER_INFO_BIDPRICE = "ssp_offerbidprice:%s:%s:%s"
  val KEY_OFFER_INFO_SDK_BIDPRICE = "ssp_sdkbid:%s:%s:%s"
  val KEY_OFFER_INFO_SMART_BIDPRICE = "ssp_smartbid:%s:%s:%s"
  val KEY_COUNTRY_CONFIG = "ssp_country_config:%s:%s"
}


object  xxxxx {
  def main(args: Array[String]): Unit = {

    var bidlist = new util.ArrayList[BidPriceList]()
    bidlist.add(new BidPriceList(34708, 346, 165, new java.math.BigDecimal(0.0001)))
    bidlist.add(new BidPriceList(38578, 374, 165, new java.math.BigDecimal(0.0005)))
    bidlist.add(new BidPriceList(39053, 346, 165, new java.math.BigDecimal(0.0001)))
    bidlist.add(new BidPriceList(7538,  474, 69,  new java.math.BigDecimal(0.0001)))

    var jedisPool = WriteToRedisHandler.initRedis()

    def writeToRedis[T](bidList: => util.ArrayList[T], rkey: T => String): Unit = {

      if (bidList != null && bidList.size > 0) {

        try {
          val jedis = jedisPool.getResource

          for (info <- bidList) {

            var k = rkey(info)
            // test
            jedis.set(k, OM.toJOSN(info))
            jedis.expire(k, 24 * 60 * 60 + Math.round(Math.random() * 100).toInt) //Math.random.toInt * 100)

            println(s"set ok , key: $k ,${OM.toJOSN(info)}")
          }
        } catch {
          case e: Exception =>
            println(e.getMessage)
        }
      }
    }

    writeToRedis[BidPriceList](bidlist,
      info=>String.format(Constants.KEY_OFFER_INFO_SDK_BIDPRICE, info.getId, info.getCountryId, info.getCarrierId))


  }
}