package com.mobikok.ssp.data.streaming.handler.dm

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017/8/4.
  */
class OfferAutoSoldoutHandler extends Handler{


//  var dwrDayTable: String = null
  var monthTable = "ssp_report_overall_dwr_month"//ssp_report_overall_dm_month" //"ssp_report_campaign_month_dm"
  val dayTable = "ssp_report_overall_dwr"

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null

  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  val AUTO_SLOTOUT_CER = "OfferAutoSoldoutHandler_autoSlotout_cer"
  val AUTO_SLOTOUT_TOPIC = "OfferAutoSoldoutHandler_autoSlotout_topic"
  val AUTO_SLOTOUT_OFFER_CHECK_CER = "OfferAutoSoldCheck_cer"
  val AUTO_SLOTOUT_OFFER_CHECK_TOPIC = "OfferAutoSoldCheck_topic"

  override def init (moduleName: String, bigQueryClient:BigQueryClient,greenplumClient:GreenplumClient, rDBConfig:RDBConfig,kafkaClient: KafkaClient, messageClient:MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient: KafkaClient,messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

//    dwrDayTable = handlerConfig.getString("dwr.table")

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

    SQLMixUpdater.init(mySqlJDBCClient)
  }

  override def handle (): Unit = {

    LOG.warn("OfferAutoSoldoutHandler started")

    //每天上午九点执行一次自动下架
    val now = CSTTime.now.addHourToDate(-9)
    val checkTime = CSTTime.now.addHourToDate(-10)

    MC.pull(AUTO_SLOTOUT_CER, Array(AUTO_SLOTOUT_TOPIC), {x=>
      val toadyNeedInit =  x.isEmpty || ( !x.map(_.getKeyBody).contains(now) )

      if(toadyNeedInit){

        val rdbt = s"(select ClickCount, Ecpc, Cr,CompanyId from OFF_OFFER_CONFIG) as t"
        hiveContext
          .read
          .jdbc(rdbUrl, rdbt, rdbProp)
          .createOrReplaceTempView("offerConfig")

        hiveContext
          .read
          //.jdbc(rdbUrl, rdbt1, rdbProp)
          .table("OFFER")
          .createOrReplaceTempView("offer")

        //ssp_report_campaign_month_dm
        val sd = sql(
          s"""
             |select
             |  *
             |from
             |  (select
             |     sol.*,
             |     if(co_o.ID = 2,2,1) as s_companyId,
             |     of.Status,
             |     of.Modes
             |  from offer of
             |  left join
             |   (
             |     select
             |       dm.offerId as offerId,
             |       sum(dm.realrevenue) as revenue,
             |       sum(dm.clickCount) as s_clickCount,
             |       CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(revenue)/cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0     as s_ecpc,
             |       CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(feeSendCount) /cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0   as s_cr
             |     from $monthTable dm
             |     group by offerId
             |   ) sol on of.ID = sol.offerId
             |   left join offer o         on o.id = sol.offerId
             |   left join campaign cam_o    on cam_o.id = o.campaignId
             |   left join advertiser ad_o   on ad_o.id = cam_o.adverId
             |   left join employee em_o     on em_o.id = ad_o.amId
             |   left join company co_o      on co_o.id = em_o.companyId
             |   where cam_o.pricemethod != 1
             |
             | ) x
             |left join offerConfig c on c.CompanyId = x.s_companyId
             |where x.Status = 1 and (x.s_clickCount > c.ClickCount and x.s_cr = c.Cr) and find_in_set("6", x.Modes) != 0
             |
         """.stripMargin)
        //where x.Status = 1 and (x.clickCount > 20000 and (x.s_ecpc < 0.001 or x.s_cr < 0.100) )
        sd.persist()

        LOG.warn("offer auto soldout","count ", sd.count(),"take 4",sd.take(4).mkString("[", ",", "]"))

        val ups = sd
          .rdd
          .collect()
          .map{x=>

            SQLMixUpdater.addUpdate("OFFER", "ID", x.getAs[Int]("offerId"), "Status", 0)

            //待删
            s"""
               | INSERT IGNORE INTO APIOFFER_LOG
               | VALUES(
               |    ${x.getAs[Int]("offerId")},
               |    5,
               |    "${CSTTime.now.date()}",
               |    ${x.getAs[Int]("s_companyId")},
               |    0,
               |    "${CSTTime.now.time()}"
               | )
        """.stripMargin
          }

        SQLMixUpdater.execute()

        LOG.warn("offer auto soldout ups", "ups",ups.take(10), "count", ups.length )
//        mySqlJDBCClient.execute("truncate TABLE APIOFFER_LOG")
        mySqlJDBCClient.executeBatch(ups)

        sd.unpersist()

      }
      MC.push(new PushReq(AUTO_SLOTOUT_TOPIC, now))
      true
    })

    //十点钟检查当天下架的offer是否有转化，若有，则重新开启offer
    MC.pull(AUTO_SLOTOUT_OFFER_CHECK_CER, Array(AUTO_SLOTOUT_OFFER_CHECK_TOPIC), {x=>
      val toadyNeedCheck =  x.isEmpty || ( !x.map(_.getKeyBody).contains(checkTime) )

      if(toadyNeedCheck){
//      if(true){

        //当天自动下架的offerId
        val checkT = s"(select A.OfferId as offerId from APIOFFER_LOG A  where A.CreateTime = '$checkTime' and Event = 5) as t"
        val df = hiveContext
          .read
          .jdbc(rdbUrl, checkT, rdbProp)

        df.createOrReplaceTempView("check")

        val collect = df.rdd.map{x => x.getAs[Int]("offerId")}
          .collect()

        LOG.warn("OfferAutoSoldoutCheck", "df count ", collect.length, "df take 4", collect.take(4).mkString("{", ",", "}"))
        var wheres = ""

        if(collect.size > 0){
          wheres =  collect.map(x => s"ID = $x")
            .mkString(" where( ", " or ", ")")
        }

        LOG.warn("OfferAutoSoldoutCheck", "wheres", wheres)

        //获取当天已下架的offer modes
        val offerT = s"(select O.ID as offerId from OFFER O $wheres and O.status = 0 ) as t"
        LOG.warn("OfferAutoSoldoutCheck", "offerSql", offerT)

        val offerdf = hiveContext
          .read
          .jdbc(rdbUrl, offerT, rdbProp)

        offerdf
          .createOrReplaceTempView("autoSoldoutOffer")
        LOG.warn("OfferAutoSoldoutCheck", "offerdf count", offerdf.collect().length, "offerdf take 4", offerdf.take(4).toList)

        val checkDf = sql(
          s"""
             |select
             |  t.offerId
             |from
             |  (select
             |    o.offerId as offerId,
             |    sum(m.conversion) as conversion
             |  from autoSoldoutOffer o
             |  left join $dayTable m on o.offerId = m.offerId
             |  where o.offerId is not null and m.b_date = '${checkTime}'
             |  group by o.offerId
             |  ) t
             |where t.conversion > 0
             |
           """.stripMargin
        )

        checkDf.persist(StorageLevel.MEMORY_ONLY)

        LOG.warn("OfferAutoSoldoutCheck ", "checkDf take 4", checkDf.take(4).mkString("{", ",", "}"), "count", checkDf.collectAsList().size() )

//        var reopenOfferSql = ""

        val sqls = checkDf
          .collect()
          .map{x =>
            val offerId = x.getAs[Int]("offerId")

            val updateLogSql =

              s"""
                 |UPDATE APIOFFER_LOG
                 |SET Event = 8,
                 |  CreateTime="${CSTTime.now.date()}",
                 |  CreateTimeDetail="${CSTTime.now.time()}"
                 |WHERE OfferId = $offerId
               """.stripMargin


            /*s"""
                               | DELETE FROM APIOFFER_LOG
                               | WHERE OfferId = $offerId and Event = 5 and CreateTime = '${checkTime}'
               """.stripMargin*/

            val reopenOfferSql =
              s"""
                 |UPDATE OFFER
                 |SET Status = 1
                 |WHERE ID = $offerId
                 |
             """.stripMargin


            (updateLogSql, reopenOfferSql)
          }


        //重新开启当天自动下架，而现在有转换的offer
        if(sqls.map(x=> x._2).length > 0){
          mySqlJDBCClient.executeBatch(sqls.map(x=> x._2),200)
        }

        LOG.warn("reOpenSqls", sqls.map(x=> x._2).toList)


        //更新APIOFFER_LOG中当天自动下架的记录()
        if(sqls.map(x=> x._1).length > 0){
          mySqlJDBCClient.executeBatch(sqls.map(x=> x._1),200)
        }

        LOG.warn("deleteSqls", sqls.map(x=> x._1).toList)

        checkDf.unpersist(true)

      }

      MC.push(new PushReq(AUTO_SLOTOUT_OFFER_CHECK_TOPIC, checkTime))
      true
    })


    LOG.warn(s"OfferAutoSoldoutHandler handle done")

  }

}
