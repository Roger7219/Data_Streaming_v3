package com.mobikok.ssp.data.streaming.handler.dm

import java.text.SimpleDateFormat
import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.mobikok.message.client.MessageClient
import com.mobikok.message.{Message, MessageConsumerCommitReq, MessagePullReq, Resp}
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Administrator on 2017/8/4.
  */
class OfferHandlerV2 extends Handler{

  private val dayBDateFormat: SimpleDateFormat = CSTTime.formatter("yyyy-MM-dd")//new SimpleDateFormat("yyyy-MM-dd")
  private val mouthFormat: SimpleDateFormat = CSTTime.formatter("yyyyMM") // new SimpleDateFormat("yyyyMM")

  var dmDayTable: String = "ssp_report_overall_dm_day"
  var topic = "ssp_report_overall_dwr"

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClient = null

  var offerHandlerConsumer = "offerHandler"

  val TOADY_NEED_INIT_CER = "OfferHandlerV2_toady_need_init_cer"
  val TOADY_NEED_INIT_TOPIC = "OfferHandlerV2_toady_need_init_topic"

  override def init (moduleName: String, bigQueryClient:BigQueryClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient:MessageClient, hbaseClient: HBaseClient, hiveContext: HiveContext, argsConfig: ArgsConfig, handlerConfig: Config, clickHouseClient: ClickHouseClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, bigQueryClient, rDBConfig, kafkaClient: KafkaClient,messageClient, hbaseClient, hiveContext, argsConfig, handlerConfig, clickHouseClient, moduleTracer)

//    dmDayTable = handlerConfig.getString("dm.table")

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

    mySqlJDBCClient = new MySqlJDBCClient(
      rdbUrl, rdbUser, rdbPassword
    )
    SQLMixUpdater.init(mySqlJDBCClient)
  }

  var prevDay: String = null
  override def doHandle (): Unit = {

    LOG.warn("OfferHandler read mysql table OFFER started")

    val ms:Resp[util.List[Message]] = messageClient.pullMessage(new MessagePullReq(
      "offerHandler",
      Array(topic)))
    val pd = ms.getPageData
    var mps =pd.map{x=>
      OM.toBean(x.getKeyBody, new TypeReference[Array[Array[HivePartitionPart]]]{})
    }
    val bs = mps.flatMap{x=>x}.flatMap{x=>x}.filter(x=>"b_date".equals(x.name)).distinct
    LOG.warn(s"OfferHandler b_date list", bs.toArray)


    //TodayCaps:计费条数, TodayFee:计费金额
    val o = hiveContext
      .read
//      .jdbc(rdbUrl, "OFFER", rdbProp)
        .table("OFFER")
      .select("ID", "TodayShowCount", "TodayClickCount", "TodayFee", "TodayCaps"/*, "ClickCount", "AmStatus","Status"*/)
      .repartition(1)
      .alias("o")

    LOG.warn("OfferHandler mysql table OFFER take(2)", o.take(2))


    val c = hiveContext
      .read
//      .jdbc(rdbUrl, "CAMPAIGN", rdbProp)
      .table("CAMPAIGN")
      .select("ID", "AdverId","ShowCount", "ClickCount", "DailyCost", "TotalCost","PriceMethod","adCategory1")
      .repartition(1)
      .alias("c")

    LOG.warn("OfferHandler mysql table CAMPAIGN take(2)", c.take(2))

    var now=CSTTime.now.date()//.format(new Date())

    //更新offer當天点击，展示，计费金额，计费条数数据,凌晨清零
//    if(!now.equals(prevDay)) {
//
//      LOG.warn("OfferHandler today data init")
//
//      val h = CSTTime.now.hour()
//      if(h <= 4) {
//        var sql =
//          s"""
//             | update OFFER
//             | set
//             | TodayClickCount = 0,
//             | TodayShowCount = 0,
//             | TodayFee = 0.0,
//             | TodayCaps = 0
//              """.stripMargin
//        mySqlJDBCClient.execute(sql)
//      }
//      prevDay = now
//    }

    // 更新offer當天点击，展示，计费金额，计费条数数据，凌晨数据清零
    MC.pull(TOADY_NEED_INIT_CER, Array(TOADY_NEED_INIT_TOPIC), {x=>
      val toadyNeedInit =  x.isEmpty || ( !x.map(_.getKeyBody).contains(now) )

      if(toadyNeedInit){

        //立即执行，避免后执行会将重置的还原了
        SQLMixUpdater.execute()

        var sql =
          s"""
             | update OFFER
             | set
             | TodayClickCount = 0,
             | TodayShowCount = 0,
             | TodayFee = 0.0,
             | TodayCaps = 0
          """.stripMargin
        mySqlJDBCClient.execute(sql)

        MC.push(new PushReq(TOADY_NEED_INIT_TOPIC +"__" + "reset", now)) //for debug,能删掉
      }
      MC.push(new PushReq(TOADY_NEED_INIT_TOPIC, now))
      true
    })


    val bx =  bs.map(x => x.value)

    if(bx.contains(now)) {
      var up:DataFrame = null
      RunAgainIfError.run{
        up = hiveContext
          .read
          .table(dmDayTable)
          .where(s"b_date = '${now}' and ( data_type = 'camapgin' or data_type is null ) and offerId is not null")
          .groupBy("offerId")
          .agg(
            max("campaignId").as("campaignId"),  // 一个offerId只对应一个campaignId
            sum("clickCount").as("clickCount"),
            sum("showCount").as("showCount"),
            sum("realRevenue").as("cost"),
            sum("conversion").as("conversion")
          )
          .alias("t")
          .join(o, col("t.offerId") === col("o.ID"), "left_outer")
          .join(c, col("t.campaignId") === col("c.ID"), "left_outer")
          .selectExpr(
            "nvl(t.offerId, o.ID) AS offerId",
            "c.PriceMethod AS priceMethod",  // 用于更新offer BidPrice
            "nvl(t.clickCount, 0) AS clickCount",
            "nvl(t.showCount, 0) AS showCount",
            "ROUND(cast(nvl(t.cost, 0.0) AS double), 4) AS cost",
            "nvl(t.conversion, 0) AS conversion"
            //        ,
            //        "o.ClickCount",
            //        "o.AmStatus",
            //        "o.Status"
          )
          .alias("up")

        up.persist()
      }


      LOG.warn(s"OfferHandler update data", "take(2)", up.take(2), "SQLMixUpdater count", SQLMixUpdater.count())

      val ups = up
        .filter(expr("clickCount != 0 or showCount != 0 or cost !=0 or conversion != 0"))
        .rdd
        .collect()
        .map{ x=>

          SQLMixUpdater.addUpdate("OFFER", "ID", x.getAs[Int]("offerId"), Array(
            KV("TodayClickCount", x.getAs[Long]("clickCount")),
            KV("TodayShowCount", x.getAs[Long]("showCount")),
            KV("TodayFee", x.getAs[Double]("cost")),
            KV("TodayCaps", x.getAs[Long]("conversion"))
          ))

          //待删
          s"""
             | update OFFER
             | set
             | TodayClickCount = ${x.getAs[Long]("clickCount")},
             | TodayShowCount = ${x.getAs[Long]("showCount")},
             | TodayFee = ROUND(${x.getAs[Double]("cost")}, 4),
             | TodayCaps = ${x.getAs[Int]("conversion")}
             | where ID = ${x.getAs[Int]("offerId")}
           """.stripMargin
        }

      up.unpersist()

//      mySqlJDBCClient.executeBatch(ups, 1000)
      LOG.warn(s"OfferHandler offer click/show/fee/caps  update done", "SQLMixUpdater count", SQLMixUpdater.count())

    }


    val h = CSTTime.now.hour()//new Date().getHours

    //更新offer BidPrice
    // 延迟问题
    if (h >= 3) {

//      val upBidPrices = upIds.map {x=>
        val upBidPrices= s"""
           | update OFFER o, CAMPAIGN c
           | set o.BidPrice =
           |  case
           |   when o.TodayFee = 0 AND o.ClickCount > 0 AND o.TodayClickCount > o.ClickCount  AND o.ClickCount>0
           |   then 0.0
           |
           |   when o.TodayFee > 0 AND o.ClickCount > 0 AND o.TodayClickCount > o.ClickCount
           |   then ROUND(if(o.TodayFee/o.TodayClickCount > 0.0001, o.TodayFee/o.TodayClickCount, 0.0001),4)
           |
           |   when o.TodayFee > 0 AND o.ClickCount > 0 AND o.TodayClickCount <= o.ClickCount AND o.TodayFee/o.TodayClickCount >0.0001 AND o.Status=1 AND o.AmStatus=1
           |   then ROUND(if(o.TodayFee/o.TodayClickCount > 0.0001, o.TodayFee/o.TodayClickCount, 0.0001),4)
           |
           |   when o.TodayFee > 0 AND o.TodayClickCount > o.ClickCount AND o.ClickCount > 0 AND 0.00001 > o.TodayFee/o.TodayClickCount
           |   then 0.0
           |
           |  else o.BidPrice
           |  end
           | where o.status = 1 and o.amStatus = 1 and c.Id = o.CampaignId and c.PriceMethod = 3
           |
           """.stripMargin
//      }
//        }
//        .collect()
      mySqlJDBCClient.execute(upBidPrices)

      LOG.warn(s"OfferHandler offer BidPrice update done")
    }


    //Redis BidPrice
    //ssp_offerbidprice:offerId:countryId:carrierId bidPrice为计费金额/点击数，保留四位小数
    val redisUp =hiveContext
      .read
//      .jdbc(rdbUrl, "OFFER", rdbProp)
      .table("OFFER")
      .select("ID", "CountryIds","CarrierIds", "BidPrice")
      .rdd
      .map{x=>

//        x.getAs[String]("CountryIds").split(",").filter(y=> !"".equals(y.trim())).foreach{z=>
//          x.getAs[String]("CarrierIds").split(",").filter(y=> !"".equals(y.trim())).foreach{k=>
//          }
//        }
        x.getAs[String]("CountryIds").split(",").filter(y=> !"".equals(y.trim())).map{z=>
          x.getAs[String]("CarrierIds").split(",").filter(y=> !"".equals(y.trim())).map{k=>k
            (s"ssp_offerbidprice:${x.getAs[Int]("ID")}:${z}:${k}", x.getAs[Double]("BidPrice").toString)
          }
        }.flatMap{m=>m}
      }.flatMap{n=>n}
    redisUp.foreach{x=>
      //CodisClient.set(x._1, x._2)
    }
    //CodisClient.set(s"ssp_offerbidprice:${x.getAs[Int]("ssp_offerbidprice")}:${z}:${k}", x.getAs[Double]("BidPrice").toString)

    //更新Top offer数据统计
    bs.foreach{x=>
      var topUp:DataFrame = null
      RunAgainIfError.run{
        topUp = hiveContext
          .read
          .table(dmDayTable)
          .where(s""" b_date = "${x.value}" """)
          .groupBy("b_date", "offerId", "adType", "countryId", "carrierId")
          .agg(
            max("campaignId").as("campaignId"),   //一个offerId只对应一个campaignId
            sum("sendCount").as("sendCount"),
            sum("clickCount").as("clickCount"),
            sum("showCount").as("showCount"),
            sum("conversion").as("conversion"),        //某个offer计费条数
            sum("realRevenue").as("cost")                   //某个offer计费金额
          )
          // 注意: ecpc原意: cost/clickCount 但这里要求是伪ecpc: cost/sendCount
          .selectExpr(
          "*",
          "sum(sendCount) over(partition by countryId, carrierId) as totalSendCount",
          "sum(clickCount) over(partition by countryId, carrierId) as totalClickCount",
          "sum(showCount) over(partition by countryId, carrierId) as totalShowCount",
          "sum(conversion) over(partition by countryId, carrierId) as totalConversion",
          "sum(cost) over(partition by countryId, carrierId) as totalCost",
          """
            | case clickCount
            | when 0 then 0.0
            | else 1.0*cost/sendCount end as ecpc
          """.stripMargin
        )
          .selectExpr("*", "row_number() over(PARTITION BY countryId, carrierId ORDER BY sendCount desc, cost desc, ecpc desc, clickCount desc, showCount desc, conversion desc) as rowNumber")
          .where("rowNumber = 1")
          .orderBy(col("sendCount").desc, col("cost").desc, col("ecpc").desc, col("clickCount").desc, col("showCount").desc, col("conversion").desc)
          .limit(100)
          .alias("res")
          .join(c, col("res.campaignId") === col("c.ID"), "left_outer")  // 关联Campaign表对应的adCategory1(成人、非成人)字段
          .selectExpr("res.*", "nvl(c.adCategory1, 0) as adCategory1")// adCategory1为0脏数据
          .selectExpr("adCategory1 as category","adType as type","countryId as countryid","carrierId as carrierid","offerId as offerid","sendCount as sendcount","clickCount as clickcount","showCount as showcount","cost as fee","totalSendCount as totalsendcount","totalClickCount as totalclickcount","totalShowCount as totalshowcount","totalCost as totalfee","conversion as feecount","totalConversion as totalfeecount","b_date as statdate")
          .alias("up")

        topUp.persist()
      }


      LOG.warn(s"OfferHandler Top offer", "count", topUp.count, "b_date", x.value, "take(2)", topUp.take(2))

      //    val b_dates = ps.flatMap{_}.flatMap{_}.filter{_.name == "b_date"}

      val _mouthFormat = mouthFormat
      val _dayBDateFormat = dayBDateFormat

//      topUp.createOrReplaceTempView("top_offer_dm")
      topUp.write.mode(SaveMode.Overwrite).insertInto("top_offer_dwr")

      topUp.unpersist()
//      LOG.warn(s"OfferHandler createOrReplaceTempView count: ${topUp.count} take(10)",topUp.take(1))
//      val ps = mps
//        .flatMap{x=>x}
//        .flatMap{x=>x}
//        .filter{x=>"b_date".equals(x.name)}
//        .distinct
//        .toArray
//      greenplumClient.overwrite("top_offer", "top_offer_dm", Array(x), "statdate")
    }

    messageClient.commitMessageConsumer(
      pd.map {d=>
        new MessageConsumerCommitReq(offerHandlerConsumer, d.getTopic, d.getOffset)
      }:_*
    )


//    val tbs = topUp
//      .rdd
//      .map{x=>
//        _mouthFormat.format(_dayBDateFormat.parse((x.getAs[String]("b_date"))))
//      }
//      .distinct(1)
//      .map{x=>
//        s"""
//          |CREATE TABLE IF NOT EXISTS `TOP_OFFER_$x` (
//          |  `StatDate` date NOT NULL,
//          |  `Category` int(11) NOT NULL DEFAULT '0',
//          |  `Type` int(11) NOT NULL DEFAULT '0',
//          |  `CountryId` int(11) NOT NULL DEFAULT '0',
//          |  `CarrierId` int(11) NOT NULL DEFAULT '0',
//          |  `OfferId` int(11) NOT NULL DEFAULT '0',
//          |  `SendCount` int(11) NOT NULL DEFAULT '0',
//          |  `ClickCount` int(11) NOT NULL DEFAULT '0',
//          |  `ShowCount` int(11) NOT NULL DEFAULT '0',
//          |  `Fee` decimal(15,4) NOT NULL DEFAULT '0.0000',
//          |  `TotalSendCount` int(11) NOT NULL DEFAULT '0',
//          |  `TotalClickCount` int(11) NOT NULL DEFAULT '0',
//          |  `TotalShowCount` int(11) NOT NULL DEFAULT '0',
//          |  `TotalFee` decimal(15,4) NOT NULL DEFAULT '0.0000',
//          |  `FeeCount` int(11) NOT NULL DEFAULT '0',
//          |  `TotalFeeCount` int(11) NOT NULL DEFAULT '0',
//          |  PRIMARY KEY (`StatDate`,`Category`,`Type`,`CountryId`,`CarrierId`)
//          |) ENGINE=InnoDB DEFAULT CHARSET=utf8
//          |
//    """.stripMargin
//      }
//      .collect()
//
//    mySqlJDBCClient.executeBatch(tbs)
//
//    val upTopOffers = topUp
//      .rdd
//      .map{ x=>
//        val tt = "TOP_OFFER_" + _mouthFormat.format(_dayBDateFormat.parse((x.getAs[String]("b_date"))))
//        s"""
//           |
//           | insert into $tt (
//           |   StatDate,
//           |   OfferId,
//           |   Type,
//           |   Category,
//           |   CountryId,
//           |   CarrierId,
//           |
//           |   TotalSendCount,
//           |   TotalShowCount,
//           |   TotalClickCount,
//           |   TotalFeeCount,
//           |   TotalFee,
//           |
//           |   SendCount,
//           |   ShowCount,
//           |   ClickCount,
//           |   FeeCount,
//           |   Fee
//           | )
//           | values(
//           |   "${x.getAs[String]("b_date")}",
//           |   ${x.getAs[Int]("offerId")},
//           |   ${x.getAs[Int]("adType")},
//           |   ${x.getAs[Int]("adCategory1")},
//           |   ${x.getAs[Int]("countryId")},
//           |   ${x.getAs[Int]("carrierId")},
//           |
//           |   ${x.getAs[Long]("totalSendCount")},
//           |   ${x.getAs[Long]("totalShowCount")},
//           |   ${x.getAs[Long]("totalClickCount")},
//           |   ${x.getAs[Long]("totalConversion")},
//           |   ${x.getAs[java.math.BigDecimal]("totalCost")},
//           |
//           |   ${x.getAs[Long]("sendCount")},
//           |   ${x.getAs[Long]("showCount")},
//           |   ${x.getAs[Long]("clickCount")},
//           |   ${x.getAs[Long]("conversion")},
//           |   ${x.getAs[java.math.BigDecimal]("cost")}
//           | ) on duplicate key update
//           |   OfferId = values(OfferId),
//           |
//           |   TotalSendCount = values(TotalSendCount),
//           |   TotalShowCount = values(TotalShowCount),
//           |   TotalClickCount = values(TotalClickCount),
//           |   TotalFeeCount = values(TotalFeeCount),
//           |   TotalFee = values(TotalFee),
//           |
//           |   SendCount = values(SendCount),
//           |   ShowCount = values(ShowCount),
//           |   clickCount = values(ClickCount),
//           |   FeeCount = values(FeeCount),
//           |   Fee = values(Fee)
//           |
//         """.stripMargin
//      }
//      .collect()
//
//    mySqlJDBCClient.executeBatch(upTopOffers)
//    LOG.warn(s"OfferHandler Top Offers update done")


    LOG.warn(s"OfferHandler handle done")

  }

  private def multiPartitionsWhereSQL(mps : mutable.Buffer[Array[Array[HivePartitionPart]]]): String = {
    var where =  mps.map{x=>
      "(" + partitionsWhereSQL(x) + ")"
    }.mkString(" or ")
    if("".equals(where)) where = "1 = 1"
    where
  }


  private def partitionsWhereSQL(ps : Array[Array[HivePartitionPart]]): String = {
    var w = ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString("(", " and ", ")")
    }.mkString(" or ")
    if ("".equals(w)) w = "1 = 1"
    w
  }


}
