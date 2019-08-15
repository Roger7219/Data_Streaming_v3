package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.util

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.http.{Entity, Requests}
import com.mobikok.ssp.data.streaming.util.{MessageClientUtil, MySqlJDBCClientV2, OM, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/11/27.
  */
class UserInfoHandler extends Handler {

  val consumer = "user_info_cer"

  var sTable : String = null
  var topics: Array[String] = null

  var requests: Requests = new Requests(1)
  var l_time : String = null

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

    sTable = handlerConfig.getString("table")
    topics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))

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
    LOG.warn("UserInfo handler starting")

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


            LOG.warn("user info ", s"""count: ${userInfo.count()}\ntake(2): ${userInfo.take(2).mkString("[","," ,"]")} """)
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
  //
  //            val context = udata.map{ x =>
  //              val info =
  //                s"""
  //                   |  ${x.getAs[java.lang.String]("imei")},
  //                   |  ${x.getAs[java.lang.String]("androidid")},
  //                   |  ${x.getAs[java.lang.String]("lon")},
  //                   |  ${x.getAs[java.lang.String]("mac1")},
  //                   |  ${x.getAs[java.lang.String]("mac2")},
  //                   |  ${x.getAs[java.lang.String]("lac")},
  //                   |  ${x.getAs[java.lang.String]("cellid")},
  //                   |  ${x.getAs[java.lang.String]("ctype")},
  //                   |  ${x.getAs[java.lang.String]("createtime")},
  //                   |  ${x.getAs[java.lang.String]("ipaddr")}
  //                   |
  //                }
  //                 """.stripMargin
  //
  //              info
  //            }

              LOG.warn("UserInfo handler send to email take(1): ",udata.take(1))

              requests.post(
                s"http://47.90.83.33:8888/postdata/push",
                new Entity().setStr(udata.mkString("[","," ,"]"), Entity.CONTENT_TYPE_JSON)//context.mkString("[","," ,"]")
              )

            }


            // 用户明細信息寫入mysql
  //          val dataForMysql = userInfo
  //            .selectExpr(
  //              "t.appId",
  //              "t.imsi",
  //              "t.imei",
  //              "t.countryId",
  //              "t.carrierId",
  //              "t.model",
  //              "t.version",
  //              "t.sdkVersion",
  //              "t.installType",
  //              "t.leftSize",
  //              "t.androidId",
  //              "t.ipAddr",
  //              "t.screen",
  //              "t.userAgent",
  //              "t.sv",
  //              "t.createTime"
  //            )
  //            .collect()

            LOG.warn("user info send  to mysql :count: ${userInfo.count()}\ntake(1):", userInfo.take(1))

  //          val sqls =
  //            s
  //               |INSERT INTO testx(appId,
  //               |        imsi,
  //               |        imei,
  //               |        countryId,
  //               |        carrierId,
  //               |        model,
  //               |        version,
  //               |        sdkVersion,
  //               |        installType,
  //               |        leftSize,
  //               |        androidId,
  //               |        ipAddr,
  //               |        screen,
  //               |        userAgent,
  //               |        sv,
  //               |        createTime
  //               | )
  //               |  VALUES(?,?,?,? ,?,?,?,? ,?,?,?,? ,?,?,?,?)
  //       """.stripMargin

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

                    LOG.warn(s"UserInfo handler sql:${OM.toJOSN(sql)} , datas take(1)", list.take(1) )
                    mySqlJDBCClient.executeBatch(sql,500, list :_*)

                    }

  //              .map{ x =>
  //                val table = "SSP_USERS_APP_" + Math.abs(x.getAs[String]("imei").hashCode % 10)
  //
  //                val sql =
  //                  s"""
  //                     | INSERT IGNORE INTO ${table}(
  //                     |        appId,
  //                     |        imsi,
  //                     |        imei,
  //                     |        countryId,
  //                     |        carrierId,
  //                     |        model,
  //                     |        version,
  //                     |        sdkVersion,
  //                     |        installType,
  //                     |        leftSize,
  //                     |        androidId,
  //                     |        ipAddr,
  //                     |        screen,
  //                     |        userAgent,
  //                     |        sv,
  //                     |        createTime
  //                     | )
  //                     |  VALUES(
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?,
  //                     |       ?
  //                     | )
  //                     |
  //                      """.stripMargin
  //                sql
  //              }


  //            val ss = sqls.toList.take(4).mkString("[", "," , "]")
  //            LOG.warn("UserInfo handler sqls take(4): ",ss)

  //            mySqlJDBCClient.executeBatch(sqls)

            }

            userInfo.unpersist()
          }

          LOG.warn("UserInfo handler done")
          return true
        }

      }, topics:_*)
    })
  }

}

object xvc{
  def main(args: Array[String]): Unit = {

    val sql =
      s"""
         |INSERT IGNORE INTO SSP_USERS_APP_8(appId,
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
         |       2028,
         |       "404446006684933",
         |       "865165033077814",
         |       97,
         |       643,
         |       "CPH1701",
         |       "6.0.1",
         |       0,
         |       0,
         |       "5698908",
         |       "3c4ba39748823ed6",
         |       "42.109.252.119",
         |       "720x1280",
         |       "Mozilla/5.0 (Linux; Android 6.0.1; CPH1701 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/63.0.3239.107 Mobile Safari/537.36 ",
         |       "v2.1.0",
         |       "2017-12-14 18:54:59"
         | )
         |
 |                      ,
         | INSERT IGNORE INTO SSP_USERS_APP_6(appId,
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
         |       2553,
         |       "639021018274367",
         |       "358808085733529",
         |       106,
         |       2,
         |       "Infinix X559",
         |       "7.0",
         |       0,
         |       0,
         |       "4296",
         |       "c9980f7b360c8b3a",
         |       "197.231.181.44",
         |       "720x1200",
         |       "Mozilla/5.0 (Linux; Android 7.0; Infinix X559 Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36 ",
         |       "v3.0.5",
         |       "2017-12-14 18:55:03"
         | )
       """.stripMargin


    val url = "jdbc:mysql://192.168.111.22:4000/kok_ssp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8"
    val user = "root"
    val password = "@dfei$@DCcsYG"


//    val rdbProp = new java.util.Properties {
//      {
//        setProperty("user", user)
//        setProperty("password", rdbPassword)
//        setProperty("driver", "com.mysql.jdbc.Driver") //must set!
//      }
//    }

    val mySqlJDBCClient = new MySqlJDBCClientV2(
      "test", url, user, password
    )

    mySqlJDBCClient.execute(sql)
//    println("Mozilla/5.0 (Linux; U; Android 4.2.2; ru-ru; DEXP Ixion E2 4\" Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30".replaceAll(""""""", """\\"""") )

  }
}