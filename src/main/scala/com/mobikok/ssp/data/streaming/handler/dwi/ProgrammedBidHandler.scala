package com.mobikok.ssp.data.streaming.handler.dwi

import java.util.Date

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.util.DateFormatUtil
import io.codis.jodis.{JedisResourcePool, RoundRobinJedisPool}

import scala.collection.JavaConversions._
import java.util
import java.util.Date

import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionRoolbackedCleanable}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import redis.clients.jedis.Jedis

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ProgrammedBidHandler extends Handler {

  val ZK_PROXY_DIR = "/zk/codis/db_kok_adv/proxy"
  val HOST_PORT = "104.250.141.178:2181,104.250.137.154:2181,104.250.128.138:2181,104.250.133.106:2181,104.250.133.114:2181"

  val AB_PERCENT_INFO = "dsp_bidab"
  val KEY_BIDPRICE_INFO = "bidconfig:%s"
  val KEY_BIDPRICE_APP_COUNTRY_PREFIX = "bidappcou:"
  val KEY_BIDPRICE_APP_COUNTRY = s"$KEY_BIDPRICE_APP_COUNTRY_PREFIX%s:%s"//集合value 格式

  val KEY_BIDPRICE_A = "bidca:%s:%s"
  val KEY_BIDPRICE_B = "bidcb:%s:%s"

  val BID_PRICE_UP_M = 1.2
  val BID_PRICE_DOWN_M = 0.6

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null
  var rdbTable: String = null

  private val jedisPool:JedisResourcePool = RoundRobinJedisPool.create().curatorClient(HOST_PORT, 30000).zkProxyDir(ZK_PROXY_DIR).build()


  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)

    rdbUrl = handlerConfig.getString(s"rdb.url")
    rdbUser = handlerConfig.getString(s"rdb.user")
    rdbPassword = handlerConfig.getString(s"rdb.password")

    rdbProp = new java.util.Properties {
      {
        setProperty("user", rdbUser)
        setProperty("password", rdbPassword)
        setProperty("driver", "com.mysql.jdbc.Driver")
      }
    }
  }

  def doHandle (newDwi: DataFrame): DataFrame= {

    LOG.warn(s"ProgrammedBidHandler handle start")

      //集合key: 用于判断是否中签的集合的key
      val rkeyBid: String = String.format(KEY_BIDPRICE_INFO, DateFormatUtil.CST("yyyy-MM-dd").format(new Date()))
      //集合value:
//      val rkeyConfig: String = String.format(KEY_BIDPRICE_APP_COUNTRY, reqInfo.getSubId, reqInfo.getCountryId) //集合
//      val rkey = String.format(KEY_BIDPRICE_A, reqInfo.getSubId, reqInfo.getCountryId)  // 返回 Case(test,price)

    //常量才能用implicits
    val hc = hiveContext
    import hc.implicits._

    var jedis: Jedis = null

    try{

      jedis = jedisPool.getResource

      var appCountrysKey = "key"

      newDwi.createOrReplaceTempView("dwi")

      //appId, 初始竞价, 最低竞价, 最高竞价
      val apps = hiveContext
        .read
        .jdbc(rdbUrl, "APP", rdbProp)
        .selectExpr(
          "id as appId",
          "cast(InitialBidPrice as double) as initPrice",
          "cast(MaxBidPrice as double) as maxPrice",
          "cast(MinBidPrice as double) as minPrice")
        .collect()
        .map{x=>
          x.getAs[Int]("appId") -> (x.getAs[Double]("initPrice"), x.getAs[Double]("minPrice"), x.getAs[Double]("maxPrice"))
        }
        .toMap
      LOG.warn("read mysql app table", "take(2)", apps.take(2), "count", apps.size)

      // cpm: priceMethod = 2
      val rt = sql(
        s"""
           |select * from(
           |  select
           |    row_number() over(partition by subId, countryId order by showTime DESC) as num,
           |    *
           |  from dwi
           |  where priceMethod = 2 and subId is not null and countryId is not null
           |)t0
           |where num = 1
         """.stripMargin
        )
      rt.createOrReplaceTempView("rt")
      LOG.warn("Read kafka latest app/country minimum bid", "take(2)", rt.take(2), "count", rt.count())

      //app/country集合
      val ac = jedis.zrange(rkeyBid, 0, -1).map{x=>
          val s = x.substring(KEY_BIDPRICE_APP_COUNTRY_PREFIX.length + 1).split(":")
          (Integer.valueOf(s(0)), Integer.valueOf(s(1)))
        }
        .toSet[(Integer, Integer)] // 为了去重
        .toSeq
        .toDF("subId","countryId")
        .where("subId is not null and countryId is not null")
      ac.createOrReplaceTempView("ac")
      LOG.warn("Read redis app/country list", "take(2)", ac.take(2), "count", ac.count())

      var hits = sql(
        s"""
           |select
           |  nvl(rt.subId, ac.subId) as subId,
           |  nvl(rt.countryId, ac.countryId) as countryId,
           |  if(rt.subId is null and rt.countryId is null, -1,
           |    if(ac.subId is null and ac.countryId is null, 1, 0)
           |  ) as hit,
           |  rt.bidPrice as hitPrice
           |from rt
           |full join ac on rt.subId = ac.subId and rt.countryId = ac.countryId
           |
         """.stripMargin)
        .collect()

      LOG.warn("Full join to return hits", "take(2)", hits.take(2), "count", hits.length)

      val testKind = 1

      hits.foreach{x=>
        val subId = x.getAs[Int]("subId")
        val countryId = x.getAs[Int]("countryId")
        val hitPrice = x.getAs[Double]("hitPrice")
        val hit = x.getAs[Int]("hit")
        val maxP = apps.get(subId).get._3
        val minP = apps.get(subId).get._2
        val initP = apps.get(subId).get._1

        //没有中签-实时数据没有对应数据
        if(hit == -1) {
          val c = readCurrElseInitBidConfig(jedis, subId, countryId, initP)
          var nc = c.bidPrice * BID_PRICE_UP_M
          if(nc > maxP) {
            nc = maxP
          }
          c.bidPrice = nc
          c.test = testKind
          updateCurrBidConfig(jedis, subId, countryId, c)
        }
        //有中签-但redis app/country配置表没有对应数据
        else if(hit == 1){
          val c = readCurrElseInitBidConfig(jedis, subId, countryId, initP)
          var nc = c.bidPrice * BID_PRICE_DOWN_M
          if(nc < minP) {
            nc = minP
          }
          c.bidPrice = nc
          c.test = testKind
          updateCurrBidConfig(jedis, subId, countryId, c)
        }
        //有中签(hit == 0)
        else  {
          val c = readCurrElseInitBidConfig(jedis, subId, countryId, initP)
          var nc = c.bidPrice * BID_PRICE_DOWN_M
          if(nc < minP) {
            nc = minP
          }
          c.bidPrice = nc
          c.test = testKind
          updateCurrBidConfig(jedis, subId, countryId, c)
        }
      }

      LOG.warn("Update redis bidConfig done")

    }catch {case e:Throwable =>
      throw e
    }finally {
      if(jedis != null) {
        jedis.close()
      }
    }

    LOG.warn(s"ProgrammedBidHandler handle done")

    newDwi

  }

  def readCurrElseInitBidConfig (jedis: Jedis, subId:Integer, countryId:Integer, initBidPrice: Double): BidConfig ={
    var k = String.format(KEY_BIDPRICE_A, subId, countryId)
    var v = jedis.get(k)
    var c: BidConfig = null
    if(StringUtil.notEmpty(v)) {
      c = OM.toBean(v, classOf[BidConfig])
      LOG.warn("read BidConfig via redis bidPrice", "subId", subId, "countryId", countryId, "bidConfig", v)
    }else {
      c = new BidConfig()
      c.bidPrice = initBidPrice
      LOG.warn("read BidConfig via mysql initBidPrice", "subId", subId, "countryId", countryId, "initBidPrice", initBidPrice)
    }
    c
  }
  def updateCurrBidConfig (jedis: Jedis, subId:Integer, countryId:Integer, newBidConfig:BidConfig ) {
    var k = String.format(KEY_BIDPRICE_A, subId, countryId)
    var v = jedis.set(k, OM.toJOSN(newBidConfig))
    LOG.warn("update BidConfig", "subId", subId, "countryId", countryId, "bidConfig", v)
  }

  override def doCommit (): Unit = {

  }

  override def doClean (): Unit = {
  }
}

class BidConfig{
  @BeanProperty var bidPrice: Double = _
  @BeanProperty var test: Int = _
}
//
//object x{
//  def main (args: Array[String]): Unit = {
//    (0 until 10).map(x=>(Integer.valueOf("1"),2)).toSet[(Integer,Int)].foreach(x=>println(x))
//
//  }
//}