package com.mobikok.ssp.data.streaming.handler.dm.offline

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message._
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{OM, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.math.Ordering

/**
  * Created by admin on 2017/11/9.
  */
class MonitorTopNHandler extends Handler {

  var dmTable = null.asInstanceOf[String]

  var topStatHandlerConsumer = "topStatHandler"
  var topics: Array[String] = null

  var topNTable = null.asInstanceOf[String]


  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

    dmTable = handlerConfig.getString("table.dm");
    topNTable = handlerConfig.getString("table.topn")
    topics = handlerConfig.getStringList("message.topics").toArray(new Array[String](0))
  }

  def  getSpecifiedDayBefore(specifiedDay:String ) = {
    val c = Calendar.getInstance()
//    val date = null.asInstanceOf[Date]
    val date = new SimpleDateFormat("yy-MM-dd").parse(specifiedDay)
    c.setTime(date)
    val day=c.get(Calendar.DATE)
    c.set(Calendar.DATE,day-1)

    val dayBefore=new SimpleDateFormat("yyyy-MM-dd").format(c.getTime())
    dayBefore
  }

  def incTopn(b_date:String, groupBy:String, dataType: Int)= {

    LOG.warn(s"where b_date is", b_date)

    val todayTopnDF = hiveContext
      .read
      .table(dmTable)
      .where(s""" b_date in ( date_sub("$b_date", 1), "${b_date}" ) """)
      .groupBy("b_date", "publisheramid", s"${groupBy}id")
      .agg(
        max(s"${groupBy}name").as(s"${groupBy}Name"),
        sum("requestcount").as("requestCount"),
        sum("clickcount").as("clickCount"),
        sum("feereportcount").as("realConversion"),
        sum("feesendcount").as("conversion"),
        sum("realrevenue").as("realRevenue"),
        sum("revenue").as("revenue")
      )
      .selectExpr(
        "*",
        """CAST( CASE clickcount  WHEN 0 THEN 0 ELSE 100000*1000*realRevenue/cast(clickcount  as double) END AS BIGINT)/100000.0 as realEcpc""".stripMargin,
        """CAST( CASE clickcount  WHEN 0 THEN 0 ELSE 100000*1000*revenue/cast(clickcount  as double) END AS BIGINT)/100000.0 as ecpc""".stripMargin,
        """CAST(
          |CASE clickCount
          |  WHEN 0 THEN 0
          |  ELSE CAST(100000 AS BIGINT)*100*realConversion /cast(clickCount  as double) END AS BIGINT)/100000.0 as realCr
        """.stripMargin,
        """CAST(
          |CASE clickCount
          |  WHEN 0 THEN 0
          |  ELSE CAST(100000 AS BIGINT)*100*conversion /cast(clickCount  as double) END AS BIGINT)/100000.0 as cr
        """.stripMargin
      )
      .alias("t")

//    LOG.warn("topStatHandler todayTopnDF schema", todayTopnDF.schema.treeString)
    LOG.warn(s"topStatHandler name:${groupBy} todayTopnDF take2", todayTopnDF.take(2))


    //昨天的
    val yDF = todayTopnDF
      .selectExpr(
        "date_add(b_date, 1) as yesterdayDate",
        s"${groupBy}id       as yesterdayId",
        "requestcount        as yesterdayRequestCount",
        "clickcount          as yesterdayClickCount",
        "realConversion      as yesterdayRealConversion",
        "conversion          as yesterdayConversion",
        "realRevenue         as yesterdayRealRevenue",
        "revenue             as yesterdayRevenue",
        "realEcpc            as yesterdayRealEcpc",
        "ecpc                as yesterdayEcpc",
        "realCr              as yesterdayRealCr",
        "cr                  as yesterdayCr"
      )
      .alias("y")

    LOG.warn(s"topStatHandler name:${groupBy} yesterdayTopnDF take2", yDF.take(2))

    val jdf = todayTopnDF
      .join(yDF, col("t.b_date") === col("y.yesterdayDate") and col(s"t.${groupBy}id") === col(s"y.yesterdayid"), "left_outer")
      .selectExpr(
        s"t.${groupBy}id as id",
        s"t.${groupBy}name as name",
        "t.publisherAmId",
        "t.requestCount",
        "t.clickCount",
        "t.realConversion",
        "t.conversion",
        "t.realRevenue",
        "t.revenue",
        "t.realEcpc",
        "t.ecpc",
        "t.realCr",
        "t.cr",
        "y.*",
        "if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue     - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )  as realRevenueInc",
        "if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue         - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 )          as revenueInc",
        "abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realRevenue - y.yesterdayRealRevenue)*100/t.realRevenue AS BIGINT)/100.0 )) as realAbsRevenueInc",
        "abs(if(t.revenue = 0, 0, CAST(CAST(100 AS BIGINT)*(t.revenue     - y.yesterdayRevenue)*100/t.revenue AS BIGINT)/100.0 ))  as absRevenueInc",
        "if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realCr               - y.yesterdayRealCr)*100/t.realCr AS BIGINT)/100.0 )     as realCrInc",
        "if(t.cr = 0, 0, CAST(CAST(100 AS BIGINT)*(t.cr                   - y.yesterdayCr)*100/t.cr AS BIGINT)/100.0 )             as crInc",
        "if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.realEcpc           - y.yesterdayRealEcpc)*100/t.realEcpc AS BIGINT)/100.0 ) as realEcpcInc",
        "if(t.ecpc = 0, 0, CAST(CAST(100 AS BIGINT)*(t.ecpc               - y.yesterdayEcpc)*100/t.ecpc AS BIGINT)/100.0 )         as ecpcInc",
        s"$dataType as data_type",
        "t.b_date"
      )
//      .orderBy(abs(col("revenueinc")).desc,abs(col("crinc")).desc,abs(col("ecpcinc")).desc)

    LOG.warn(s"topStatHandler name:${groupBy} joinDf take2", jdf.take(2))
//    LOG.warn("topStatHandler joinDf schema", jdf.schema.treeString )

    jdf
//    .limit(topN)
    .coalesce(1)
    .write
    .format("orc")
    .mode(SaveMode.Overwrite)
    .insertInto(topNTable)

    //消息队列发送 更新通知，bq将获取该通知
    messageClient.pushMessage(new MessagePushReq(topNTable, OM.toJOSN(Array(Array(HivePartitionPart("b_date",b_date))) ) ) )

  }

  override def handle (): Unit = {
    LOG.warn("TopStatHandler starting!")

    val ms:Resp[util.List[Message]] = messageClient.pullMessage(new MessagePullReq(topStatHandlerConsumer, topics))

    val pd = ms.getPageData

    var mps =pd.map{x=>
      OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
    }

    val bs = mps
      .flatMap{x=>x}
      .flatMap{x=>x}
      .filter(x=>"b_date".equals(x.name))
      .distinct
      .sortBy(_.value) (Ordering.String.reverse)
      .take(5)

    LOG.warn(s"topStatHandler b_date list", bs.toArray)

    bs.foreach{b_date =>
      val yesterday = getSpecifiedDayBefore(b_date.value)

      RunAgainIfError.run(incTopn(yesterday, "publisher", 1))

      RunAgainIfError.run(incTopn(yesterday, "app", 2))

      RunAgainIfError.run(incTopn(yesterday, "country", 3))

      RunAgainIfError.run(incTopn(yesterday, "offer", 4))

    }

    messageClient.commitMessageConsumer(
      pd.map {d=>
        new MessageConsumerCommitReq(topStatHandlerConsumer, d.getTopic, d.getOffset)
      }:_*
    )

    LOG.warn("TopStatHandler done")
  }

}


