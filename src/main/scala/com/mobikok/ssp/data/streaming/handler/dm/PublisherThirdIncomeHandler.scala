package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{DataFrameUtil, OM}
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._

/**
  * Created by admin on 2017/9/18.
  */
class PublisherThirdIncomeHandler extends Handler{

  var rdbUrl:String = null
  var rdbUser:String = null
  var rdbPassword:String = null
  var rdbProp:java.util.Properties = null
  val consumer = "PublisherThirdIncome"
  val topic = "PublisherThirdIncome"
  val topicForDMReflush = "PublisherThirdIncomeDMReflush"

  val hiveTable = "ssp_report_publisher_third_income"
  val mysqlTablePrefix = "THIRD_INCOME_"
  //  val cubeName = "model_ssp_report_app_dm"

  override def init (moduleName: String, bigQueryClient:BigQueryClient, greenplumClient: GreenplumClient, rDBConfig:RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient,greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, argsConfig, handlerConfig)

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

  }


  override def handle(): Unit = {

    //get time from messgq
    val datas = messageClient.pullMessage(new MessagePullReq(consumer, Array(topic))).getPageData

    val pushTime = datas.map(x => x.getKeyBody).toArray

    if(pushTime.length == 0) {
      LOG.warn("PublisherThirdIncomeHandler skip")
      return
    }

    LOG.warn("PublisherThirdIncomeHandler start")

    pushTime.foreach{statDate=>
      LOG.warn("PublisherThirdIncomeHandler pull message, statDate :", statDate)
      val s = statDate.split("-")
      //get mysql table name from time
      val mysqlTable = mysqlTablePrefix + s(0) + s(1)
      LOG.warn("PublisherThirdIncomeHandler get mysqlTable name ", mysqlTable)
      //write data to hive table from mysql
      //val rdbt = s"(select * from ${mysqlTable} where statDate = '${statDate}') as t0"
      var df = hiveContext
        .read
        .jdbc(rdbUrl, /*rdbt*/mysqlTable, rdbProp)
        .where(s"statDate = '${statDate}'")
        .coalesce(1)
        .select("JarCustomerId", "AppId", "CountryId", "Pv", "ThirdFee", "ThirdSendFee", "StatDate")

      LOG.warn("data from mysql", "mysqlTable", mysqlTable, "statDate", statDate, "df", DataFrameUtil.showString(df, 500))

      df.write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .insertInto(hiveTable)

      val key = OM.toJOSN(Array(Array(HivePartitionPart("b_date", statDate), HivePartitionPart("b_time", s"$statDate 00:00:00"))))
      messageClient.pushMessage(new MessagePushReq(topicForDMReflush, key))

      //send messg to kylin for refresh cube
//      messageClient.pushMessage(new MessagePushReq(
//        topicForKylin,
//        s"""
//           |  [ [ {
//           |      \"name\" : \"b_date\",
//           |      \"value\" : \"$statDate\"
//           |    }, {
//           |      \"name\" : \"l_time",
//           |      \"value\" : \"2000-01-01 00:00:00\"
//           |  } ]]
//           |
//      """.stripMargin,
//        true,
//        ""
//      ))
    }


//    greenplumClient.overwrite(
//      "ssp_report_publisher_third_income_dm",
//      "ssp_report_publisher_third_income_dm"
//    )

    //commit messg offset
    messageClient.commitMessageConsumer(datas.map{x=>new MessageConsumerCommitReq(consumer, topic, x.getOffset)}:_* )

    LOG.warn("PublisherThirdIncomeHandler done")
  }

}
//
//object  X{
//
//  def main(args: Array[String]): Unit = {
//
//    println(OM.toJOSN("123").replaceAll("\"", "'"))
//    println(OM.toJOSN(123).replaceAll("\"", "'"))
//    println(OM.toJOSN(null).replaceAll("\"", "'"))
//
//
//
//    //    var messageClient: MessageClient = new MessageClient("http://node14:5555");
////
////    val statDate = "2017-09-02"
////
////    messageClient.pushMessage(new MessagePushReq(
////      "PublisherThirdIncomeForKylinReflush",
////      s"""
////         |[ [ {
////         |"name" : "b_date",
////         |"value" : "$statDate"
////         |}, {
////         |"name" : "l_time",
////         |"value" : "$statDate 00:00:00"
////         |} ]]
////      """.stripMargin,
////      true,
////      ""
////    ))
//  }
//}

