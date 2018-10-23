package com.mobikok.ssp.data.streaming.handler.dm

import java.io.{InputStream, OutputStream}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.sql.ResultSet
import java.util
import java.util.{ArrayList, List}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util.http.{Entity, Form, Requests, UrlParam}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

import collection.JavaConversions._
import scala.math.Ordering

/**
  * Created by admin on 2017/9/4.
  */
class MonitorOfferEcpcAndCrHandler extends Handler {

  val consumer = "monitor_offer_cer"
  var dmTable : String = null
  var topics: Array[String] = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  var requests: Requests = new Requests(1)

  var lastSendEmailTime = 0L

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    dmTable = handlerConfig.getString("table")
    topics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))

    val rdbUrl = handlerConfig.getString(s"rdb.url")
    val rdbUser = handlerConfig.getString(s"rdb.user")
    val rdbPassword = handlerConfig.getString(s"rdb.password")

    mySqlJDBCClient = new MySqlJDBCClientV2(
      moduleName, rdbUrl, rdbUser, rdbPassword
    )
    SQLMixUpdater.init(mySqlJDBCClient)
  }


  override def handle (): Unit = {
    LOG.warn("MonitorOfferEcpcAndCrHandler handler starting")

    RunAgainIfError.run({
      MessageClientUtil.pullAndSortByBDateDescHivePartitionParts(messageClient, consumer, new MessageClientUtil.Callback[util.ArrayList[HivePartitionPart]]{

        def doCallback (ps: util.ArrayList[HivePartitionPart]): java.lang.Boolean = {
          var isSendedEmail = false
          ps.foreach{x=>

            LOG.warn("where b_date", x.value)

            val tv = mySqlJDBCClient.executeQuery("select Ecpc, Cr, EcpcRevenue, CrRevenue from OFFER_WARNING_CONFIG", new Callback[(Double, Double,Long, Double)] {
              def onCallback(rs: ResultSet): (Double, Double,Long, Double) ={
                if(rs.next())
                  (
                    if(rs.getDouble("Ecpc") == null) 2.0 else rs.getDouble("Ecpc"),
                    if(rs.getDouble("Cr") == null) 2.0 else rs.getDouble("Cr"),

                    if(rs.getDouble("EcpcRevenue") == null) 10 else  rs.getLong("EcpcRevenue"),
                    if(rs.getDouble("CrRevenue") == null) 30 else  rs.getDouble("CrRevenue")
                  )
                else (2.0, 1, 10, 30)
              }
            })

            var ecpcThresholdValue = tv._1
            var crThresholdValue = tv._2/100
            var ecpcRevenueThresholdValue = tv._3
            var crRevenueThresholdValue = tv._4

            LOG.warn(s"ecpcThresholdValue: $ecpcThresholdValue, crThresholdValue: $crThresholdValue, ecpcRevenueThresholdValue: $ecpcRevenueThresholdValue, crRevenueThresholdValue: $crRevenueThresholdValue")

            var df = hiveContext
              .read
              .table(dmTable)
              .where(s""" b_date = "${x.value}" """)
              .groupBy("b_date", "adverid", "campaignid", "offerid")
              .agg(
                expr("sum(realRevenue) as realRevenue"),
                expr("sum(clickCount) as clickCount"),
                expr("sum(conversion) as conversion"),
                expr("CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(10000 AS BIGINT)*1000*sum(realRevenue)/cast(sum(clickCount) as DOUBLE) END AS BIGINT)/10000.0  as realEcpc"),
                expr("CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 10000*sum(conversion)/cast(sum(clickCount) as DOUBLE) END AS BIGINT)/10000.0 as realCr")
              )
              .where(s" (realEcpc >= $ecpcThresholdValue and realRevenue >= $ecpcRevenueThresholdValue) or ( realRevenue >= $crRevenueThresholdValue and realCr >= $crThresholdValue) ")
              .orderBy(col("realEcpc").desc, col("realCr").desc, col("realRevenue").desc, col("conversion").desc)
              .alias("d")

            df.persist()

            LOG.warn("df take(2)", df.take(2))

            var title = "Offer Ecpc Cr预警"
            var emailContent = DataFrameUtil.showString(df, 1000).replaceAll("\n", "<br>").replaceAll(" ", "&nbsp;")
            emailContent = "<div style='font-family: Consolas,Monaco,monospace' >" + emailContent + "</div>"
  //          emailContent = URLEncoder.encode(emailContent)
  //          title = URLEncoder.encode(title)


            // 发预警邮件
            if(System.currentTimeMillis() - lastSendEmailTime >= 1000*60*60*8) {
              isSendedEmail = true
              requests.post(
                s"http://ssp.mobikok.com/inter/SendEmail!sendEmail.action",
                new Entity()
                  .addForm("title", title)
                  .addForm("content", emailContent)
                  .addForm("recipientAddress", "2060878177@qq.com")
              )
            }


            // API offer里的超CR阈值处理
            val apiDF = hiveContext
              .read
              .table("offer")
              .selectExpr("id", "isapi")
              .where("isapi = 1")
              .alias("o")

            //      var thresholdValue = null.asInstanceOf[Double];
            //      // 从配置cr是百分值(百分号前的数值)
            //      //mySqlJDBCClient.executeQuery("SELECT * FROM OFFER_WARNING_CONFIG", new Callback[Double] {
            //        def onCallback(rs: ResultSet):Double = {
            //          if(rs.next()) rs.getDouble("Cr") else 1.0  //默认为1%
            //        }
            //      })

            val apiO = df
              .join(apiDF, col("d.offerid") === col("o.id"),  "left_outer")
              .where(expr(s"o.id is not null and d.realCr > $crThresholdValue"))

            LOG.warn("apiOffer", "take(2)", apiO.take(2), "SQLMixUpdater count", SQLMixUpdater.count())

            val setCR = apiO
              .collect()
              .map{x=>
                SQLMixUpdater.addUpdate("OFFER", "ID", x.getAs[Int]("id"), "Cr", 1)
                //待删
                s"UPDATE OFFER SET Cr = 1 where id = ${x.getAs[Double]("id")}"
              }
            LOG.warn("SQLMixUpdater count", SQLMixUpdater.count())

            df.unpersist()
//            mySqlJDBCClient.executeBatch(setCR)
          }
          if(isSendedEmail) {
            lastSendEmailTime = System.currentTimeMillis()
          }

          return true
        }
      }, topics:_*)
    })

//    val pd = messageClient.pullMessage(new MessagePullReq(consumer, topics)).getPageData
//    val ps = pd
//      .map{x=>
//        OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
//      }
//      .flatMap{x=>x}
//      .flatMap{x=>x}
//      .filter{x=>"b_date".equals(x.name)}
//      .distinct
//      .sortBy(_.value) (Ordering.String.reverse)
//      .take(5)


//    messageClient.commitMessageConsumer( pd.map {x=>
//      new MessageConsumerCommitReq(consumer, x.getTopic, x.getOffset)
//    }:_*)

    LOG.warn("MonitorOfferEcpcAndCrHandler handler done")
  }



}

//object xxx{
//  var requests: Requests = new Requests(1)
//
//  def main (args: Array[String]): Unit = {
//    var title = "Offer Ecpc我d"
//    var emailContent = "%E4%B我8%BA"
////    emailContent = URLEncoder.encode(emailContent, "utf-8")
////    title = URLEncoder.encode(title)
//
//    // http://192.168.1.4:8080/kok_adv_ssp_op_bigdata/inter/SendEmail!sendEmail.action?title=test&content=%E4%B8%BA&recipientAddress=2060878177@qq.com
//
//    requests.post(
//      s"http://ssp.mobikok.com/inter/SendEmail!sendEmail.action/SendEmail!sendEmail.action?recipientAddress=2060878177@qq.com",
//        new Entity().addForm("title", title).addForm("content", emailContent)
//    )
//
////
////    requests.get(
////      s"http://big.mobikok.com/inter/SendEmail!sendEmail.action/SendEmail!sendEmail.action?title=test&content=23我3我&recipientAddress=2060878177@qq.com"
////      //,
//////      new UrlParam()
//////        .addParam("title", title)
//////        .addParam("content", emailContent)
//////        .addParam("recipientAddress", "2060878177@qq.com")
////    )
//
//
//  }
//}

