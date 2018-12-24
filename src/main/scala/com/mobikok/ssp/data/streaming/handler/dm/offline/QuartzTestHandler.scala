package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.http.Requests
import com.mobikok.ssp.data.streaming.util.{MessageClientUtil, MySqlJDBCClientV2, OM, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2018/9/12 0012.
  */
class QuartzTestHandler extends Handler{
  val consumer = "quartz_cer"//"user_info_cer"

  var sTable : String = null
  var topics: Array[String] = null

  var requests: Requests = new Requests(1)
  var l_time : String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    sTable = handlerConfig.getString("table")
    topics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))

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

    mySqlJDBCClient = new MySqlJDBCClientV2(
      moduleName, rdbUrl, rdbUser, rdbPassword
    )
  }

  override def handle (): Unit = {
    LOG.warn("QuartzTestHandler handler starting")

    RunAgainIfError.run({

      MessageClientUtil.pullAndSortByLTimeDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.ArrayList[HivePartitionPart]]{

        override def doCallback(resp: util.ArrayList[HivePartitionPart]): java.lang.Boolean = {

          LOG.warn("MessageClientUtil doCallback starting!")

          if(resp.isEmpty) {
            LOG.warn("resp is null !!!!!!!!!!")
            return false
          }

          if(resp.tail.isEmpty) {
            LOG.warn("l_times tail is empty")
            return false
          }

          resp.tail.foreach{ x =>
            LOG.warn("where l_time", x.value)

            val userInfo = hiveContext
              .read
              .table(sTable)
              .selectExpr(
                "appid","imsi","imei","countryid","carrierid","model",
                "version","sdkversion","installtype","leftsize","androidid",
                "ipaddr","screen","useragent", "sv","createtime","lon","lat",
                "mac1","mac2","lac","cellid","ctype"
              )
              .where(s""" "${x.value}" = l_time """)
              .alias("t")
              .persist()


            LOG.warn("user info ", s"""count: ${userInfo.count()}\ntake(4): ${userInfo.take(4).mkString("[","," ,"]")} """)
            val gSize = 50000
            //分组数量
            val gCount = Math.ceil(1.0 * userInfo.count() / gSize)


            // 发送用户信息
            userInfo.selectExpr(
              "t.imei",
              "t.androidid",
              "t.lon",
              "t.lat",
              "t.mac1",
              "t.mac2",
              "t.lac",
              "t.cellid",
              "t.ctype",
              "t.createtime",
              "t.ipAddr")
              .toJSON.collect().zipWithIndex.groupBy(x => x._2 % gCount).foreach { y =>
                val udata = y._2.map(z=>z._1)

                LOG.warn("QuartzTestHandler handler send to email take(4): ",udata.take(4).toList)

            }

            LOG.warn("user info send  to mysql :count: ${userInfo.count()}\ntake(4):", userInfo.take(4))

            userInfo.collect().zipWithIndex.groupBy(x => x._2 % gCount).foreach { y =>

              val udata = y._2.map(z=>z._1)
              val sqls = udata
                .map{x=>
                  (
                    "SSP_USERS_APP_" + Math.abs(x.getAs[String]("imei").hashCode % 10),
                    x
                  )
                }
                .groupBy(_._1)
                .foreach{x=>
                  val table = x._1
                  val list = x._2.map{y=>
                    Map(
                      1      -> y._2.getAs[Integer]("appid"),
                      2      -> y._2.getAs[java.lang.String]("imsi"),
                      3      -> y._2.getAs[java.lang.String]("imei"),
                      4      -> y._2.getAs[Integer]("countryid"),
                      5      -> y._2.getAs[Integer]("carrierid"),
                      6      -> y._2.getAs[java.lang.String]("model"),
                      7      -> y._2.getAs[java.lang.String]("version"),
                      8      -> y._2.getAs[Integer]("sdkversion"),
                      9      -> y._2.getAs[Integer]("installtype"),
                      10     -> y._2.getAs[java.lang.String]("leftsize"),
                      11     -> y._2.getAs[java.lang.String]("androidid"),
                      12     -> y._2.getAs[java.lang.String]("ipaddr"),
                      13     -> y._2.getAs[java.lang.String]("screen"),
                      14     -> y._2.getAs[java.lang.String]("useragent"),
                      15     -> y._2.getAs[java.lang.String]("sv"),
                      16     -> y._2.getAs[java.lang.String]("createtime")
                    )}

                  val sql =
                    s"""
                       | INSERT IGNORE INTO ${table}(
                       |        appId,
                       |        imsi,
                       |        imei,
                       |        countryId,
                       |        carrierId,
                       |        model,
                       |        version,
                       |        sdkVersion,
                       |        installType,
                       |        leftSize,
                       |        androidId,
                       |        ipAddr,
                       |        screen,
                       |        userAgent,
                       |        sv,
                       |        createTime
                       | )
                       |  VALUES(
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?,
                       |       ?
                       | )
                       |
                    """.stripMargin

                  LOG.warn(s"QuartzTestHandler handler sql:${OM.toJOSN(sql)} , datas take(4)", list.take(4).toList )

                }


            }

            userInfo.unpersist()
          }

          LOG.warn("QuartzTestHandler handler done")
          false
        }

      }, topics:_*)
    })
  }



  /*
  var module = ""
  override def init(moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    module = moduleName
  }

  override def handle(): Unit = {

    LOG.warn(s"QuartzTestHandler [$module] starting!!!")

    Thread.sleep(1000*60*5)

    LOG.warn(s"QuartzTestHandler [$module] end!!!")
  }*/
}
