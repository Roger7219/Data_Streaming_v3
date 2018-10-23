package com.mobikok.ssp.data.streaming.handler.dm

import java.text.SimpleDateFormat

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by admin on 2018/06/05.
  */
class SmartLinkOfferSoldoutHandler extends Handler {

  val SMARTLINK_OFFER_SOLDOUT_CER = "SmartLinkOfferSoldoutHandler_cer"
  val SMARTLINK_OFFER_SOLDOUT_TOPIC = "SmartLinkOfferSoldoutHandler_topic"

  val SMARTLINK_OFFER_CHECK_CER = "SmartLinkOfferCheck_cer"
  val SMARTLINK_OFFER_CHECK_TOPIC = "SmartLinkOfferCheck_topic"

  val DmTable = "ssp_report_overall_dwr"
  val monthTable = "ssp_report_overall_dwr_month"

  var rdbUrl: String = null
  var rdbUser: String = null
  var rdbPassword: String = null
  var rdbProp: java.util.Properties = null
  var mySqlJDBCClient: MySqlJDBCClientV2 = null

  override def init (moduleName: String, bigQueryClient: BigQueryClient, greenplumClient: GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config): Unit = {
    super.init(moduleName, bigQueryClient, greenplumClient, rDBConfig, kafkaClient, messageClient, kylinClientV2, hbaseClient, hiveContext, handlerConfig)

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
    LOG.warn("SmartLinkOfferSoldout handler starting")

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val updateTime = CSTTime.now.addHourToDate(-8)
    val checkTime = CSTTime.now.addHourToDate(-10)

//    val today = CSTTime.now.date()

    //每天上午八点执行一次smartlink offer下架
    MC.pull(SMARTLINK_OFFER_SOLDOUT_CER, Array(SMARTLINK_OFFER_SOLDOUT_TOPIC), {x=>
      val toadyNeedUpdate =  x.isEmpty || ( !x.map(_.getKeyBody).contains(updateTime) )

      if(toadyNeedUpdate){
//      if(true){//test

        val offerT =
          s"""
             |(SELECT
             |    O.ID as offerId, O.CreateTime as createTime, O.CountryIds as countryIds, O.Modes as modes
             |FROM OFFER O
             |LEFT JOIN CAMPAIGN C ON C.ID = O.CampaignId
             |LEFT JOIN ADVERTISER A ON A.ID =C.AdverId
             |WHERE O.Status=1 AND O.AmStatus=1 AND C.Status=1 AND O.Modes LIKE CONCAT('%,',2,',%') AND C.AdverId!=0
             |) as t0
           """.stripMargin

        hiveContext
          .read
          .jdbc(rdbUrl, offerT, rdbProp)
          .as("o")
          .selectExpr(
            "o.offerId",
            "date_format(o.createTime,'yyyy-MM-dd HH:mm:ss') as createTime",
            "o.countryIds",
            "o.modes")
          .createOrReplaceTempView("offer")

        val confT = s"(select C.CountryId as countryId, C.ClickCount as clickCount from COUNTRY_CONFIG_TEST C ) as t1"
        hiveContext
          .read
          .jdbc(rdbUrl, confT, rdbProp)
          .createOrReplaceTempView("countryClickConfig")

        //offer下架条件1（Offer的存活时间大于24*8小时（当前时间与Offer的创建时间之差）且CR=0。）
        val sd1 = sql(
            s"""
               |select
               |  t0.offerId,
               |  o.createTime,
               |  o.countryIds,
               |  o.modes
               |from
               |offer o left join
               |  (select
               |    t.offerId,
               |    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE CAST(100000 AS BIGINT)*100*sum(realRevenue) /cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0  as ecpc
               |  from $monthTable t
               |  group by t.offerId) t0 on t0.offerId = o.offerId
               |where t0.offerId is not null and t0.ecpc = 0
               |
             """.stripMargin
            )

        sd1.persist()

        LOG.warn("smartlink offer soldout","count ", sd1.count(),"take 4",sd1.take(4).mkString("[", ",", "]"))


        val ups= sd1
          .rdd
          .collect()
          .map{x => smartlinkOffer(x.getAs[Int]("offerId"), x.getAs[String]("createTime"), x.getAs[String]("countryIds"), x.getAs[String]("modes"))}
          .toList
          .filter{y => y.countryIds.split(",").filter(x => !x.isEmpty).size == 1 }
          .filter{d => (System.currentTimeMillis() - sdf.parse(d.createTime).getTime())/3600/1000 >= 24*8}

        LOG.warn("smartlink offer soldout ups", "ups",ups.map(x=>s"(offerId: ${x.offerId}, createTime: ${x.createTime}, countryIds: ${x.countryIds}, modes:${x.modes})").take(4).mkString("{", ",", "}"), "count", ups.count(x => x.!=(null)) )

        val ups1 = ups
          .map{z =>
            if(z.modes.contains("2") && 1 == z.modes.split(",").filter(x => !x.isEmpty).size ){
              SQLMixUpdater.addUpdate("OFFER", "ID", z.offerId, "Status", 0)

            }else{
              SQLMixUpdater.addUpdate("OFFER", "ID", z.offerId, "Modes", z.modes.replace("2,", ""))

            }

            //smartlink下架信息写入表

            s"""
               | INSERT IGNORE INTO APIOFFER_LOG
               | VALUES(
               |    ${z.offerId},
               |    5,
               |    "${CSTTime.now.date()}",
               |    0,
               |    0,
               |    "${CSTTime.now.time()}"
               | )
            """.stripMargin
          }
          /*.map{z =>
            s"""
               |updata OFFER
               |set Modes = ${z.modes.replace(",2,", ",")}
               |where ID = ${z.offerId}
               |
               """.stripMargin
          }*/


        SQLMixUpdater.execute()
        LOG.warn("smartlink offer soldout ups1", "ups1",ups1.take(4).mkString("{", ",", "}"), "count", ups1.length )
        mySqlJDBCClient.executeBatch(ups1.toArray,200)

        /*//下架条件2
          //a)	Offer 24*8天点击数大于测试点击数(会有一个配置表在数据库当中)。
          //b)	Offer的存活时间大于10*24小时（当前时间与Offer的创建时间之差）。
          //c)	以7天为单位，计算该国家的平均ecpc = m，如果Offer在该国家的ecpc小于1/3m。

        //计算国家平均ecpc
        sql(
          s"""
             |select
             |  countryId,
             |  CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0  as avgecpc
             |from $DmTable
             |where b_date >= '${CSTTime.now.addHourToDate(-7*24)}' and b_date < '${CSTTime.now.date()}'
             |group by countryId
             |
           """.stripMargin
        ).createOrReplaceTempView("avgecpc")


//        val sd2 = sql(
//          s"""
//             |select
//             |  t1.offerId,
//             |  o.createTime,
//             |  o.countryIds,
//             |  o.modes
//             |from
//             |  (select
//             |    offerId,
//             |    countryId,
//             |    sum(clickCount) as clickCount,
//             |    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0  as ecpc
//             |  from $DmTable
//             |  group by offerId, countryId
//             |  where b_date >= '${CSTTime.now.addHourToDate(-8*24)}' and b_date < '${CSTTime.now.date()}') t1
//             |--left join countryClickConfig c on t1.countryId = c.countryId
//             |left join offer o on t1.offerId = o.offerId
//             |left join avgecpc ac on t1.countryId = ac.countryId
//             |where o.offerId is not null and ac.countryId is not null and t1.clickCount > 10000  and t1.ecpc < 1/3*ac.avgecpc--c.clickCount
//             |
//           """.stripMargin
//        )

        val sd2 = sql(
          s"""
             |select
             |  t1.offerId,
             |  o.createTime,
             |  o.countryIds,
             |  o.modes
             |from
             |  (select
             |    offerId,
             |    countryId,
             |    sum(clickCount) as clickCount,
             |    CAST( CASE sum(clickCount) WHEN 0 THEN 0 ELSE 100000*1000*sum(realRevenue)/cast(sum(clickCount)  as DECIMAL(19,10) ) END AS BIGINT)/100000.0  as ecpc
             |  from $DmTable
             |  where b_date >= '${CSTTime.now.addHourToDate(-8*24)}' and b_date < '${CSTTime.now.date()}'
             |  group by offerId, countryId) t1
             |left join offer o on t1.offerId = o.offerId
             |left join avgecpc ac on t1.countryId = ac.countryId
             |where o.offerId is not null and ac.countryId is not null and t1.clickCount > 10000  and t1.ecpc < 1/3*ac.avgecpc
             |
           """.stripMargin
        )

        sd2.persist()

        val ups2= sd2
          .rdd
          .collect()
          .map{x => smartlinkOffer(x.getAs[Int]("offerId"), x.getAs[String]("createTime"), x.getAs[String]("countryIds"), x.getAs[String]("modes"))}
          .toList
          .filter{y => y.countryIds.split(",").filter(x => !x.isEmpty).size == 1 }
          .filter{d => (System.currentTimeMillis() - sdf.parse(d.createTime).getTime())/3600/1000 >= 24*10}

        LOG.warn("smartlink offer soldout ups2", "ups2",ups2.map(x=>s"(offerId: ${x.offerId}, createTime: ${x.createTime}, countryIds: ${x.countryIds}, modes:${x.modes})").take(10).mkString("{", ",", "}"), "count", ups2.count(x => x.!=(null)) )


        val ups3 = ups2

          //          .map{z =>
          //            SQLMixUpdater.addUpdate("OFFER", "ID", z._1, "Modes", z._4)
          //
          //          }
          .map{z =>
          s"""
             |updata OFFER
             |set Modes = ${z.modes.replace(",2,", ",")}
             |where ID = ${z.offerId}
             |
             """.stripMargin
          }

        LOG.warn("smartlink offer soldout ups3", "ups3", ups3.toSet.take(10).mkString("{", ",", "}"), "count", ups3.toSet.size )


        //合并两个条件结果,总下架的offer数
        val ups4 = (ups.map{x=> x.offerId} ++ ups2.map{x=> x.offerId}).toSet

        LOG.warn("smartlink offer soldout ups4", "ups4", ups4.mkString("{", ",", "}"),"count", ups4.size )


        val ups5 = ups1 ++ ups3.filter(x=> !ups.map{y=> y.offerId.toString}.contains(x.split("where ID = ")(1)))
        LOG.warn("smartlink offer soldout ups5", "ups5", ups5.take(10).mkString("{", ",", "}"), "count", ups4.size )


        //下架，更新数据库（去除Modes字段中“,2,”字符）

        //        mySqlJDBCClient.executeBatch(ups1)
//                mySqlJDBCClient.executeBatch(ups4.toArray,200)


        sd2.unpersist()*///(condition 2)
        sd1.unpersist()
      }

      MC.push(new PushReq(SMARTLINK_OFFER_SOLDOUT_TOPIC, updateTime))
      true
    })

    //十点钟检查当天下架的offer是否有转化，若有，则重新开启offer
    MC.pull(SMARTLINK_OFFER_CHECK_CER, Array(SMARTLINK_OFFER_CHECK_TOPIC), {x=>
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

        LOG.warn("SmartLinkOfferCheck", "df count ", collect.length, "df take 4", collect.take(4).mkString("{", ",", "}"))
        var wheres = ""

        if(collect.size > 0){
          wheres =  collect.map(x => s"ID = $x")
            .mkString(" where( ", " or ", ")")
        }

        LOG.warn("SmartLinkOfferCheck", "wheres", wheres)

        //获取当天已下架的offer modes
        val offerT = s"(select O.ID as offerId, O.Modes as modes from OFFER O $wheres and status = 1) as t"
        LOG.warn("SmartLinkOfferCheck", "offerSql", offerT)

        val offerdf = hiveContext
          .read
          .jdbc(rdbUrl, offerT, rdbProp)

        offerdf
          .createOrReplaceTempView("offer")
        LOG.warn("SmartLinkOfferCheck", "offerdf count", offerdf.collect().length, "offerdf take 4", offerdf.take(4).toList)

        val checkDf = sql(
          s"""
             |select
             |  t.offerId,
             |  t.modes
             |from
             |  (select
             |    o.offerId as offerId,
             |    sum(m.conversion) as conversion,
             |    max(o.modes) as modes
             |  from offer o
             |  left join $DmTable m on o.offerId = m.offerId
             |  where o.offerId is not null and m.b_date = '${checkTime}'
             |  group by o.offerId
             |  ) t
             |where t.conversion > 0
             |
           """.stripMargin
        )

        checkDf.persist(StorageLevel.MEMORY_ONLY)

        LOG.warn("SmartLinkOfferCheck ", "checkDf take 4", checkDf.take(4).mkString("{", ",", "}"), "count", checkDf.collectAsList().size() )

        var reopenOfferSql = ""

        val sqls = checkDf
          .collect()
          .map{x =>
            val offerId = x.getAs[Int]("offerId")

            val oldModes= x.getAs[String]("modes")

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
                 | WHERE OfferId = $offerId and Event = 5 and CreateTime = ${CSTTime.now.date()}
               """.stripMargin*/


            if(! oldModes.contains(",2")){

              val tmp = oldModes.split(",").filter(x => !x.isEmpty).toList :+ "2"

              val modes = tmp.sorted.mkString(",", ",", ",")
              reopenOfferSql =
                s"""
                   |UPDATE OFFER
                   |SET Modes = $modes
                   |WHERE ID = $offerId
                   |
               """.stripMargin
            }

            (updateLogSql, reopenOfferSql)
          }


        //重新开启当天自动下架，而现在有转换的offer
        if(sqls.map(x=> x._2).length > 0){
          mySqlJDBCClient.executeBatch(sqls.map(x=> x._2),200)
        }

        LOG.warn("reOpenSqls", sqls.map(x=> x._2).toList)


        //更新APIOFFER_LOG中当天自动下架的记录
        if(sqls.map(x=> x._1).length > 0){
          mySqlJDBCClient.executeBatch(sqls.map(x=> x._1),200)
        }

        LOG.warn("deleteSqls", sqls.map(x=> x._1).toList)

        checkDf.unpersist(true)

      }

      MC.push(new PushReq(SMARTLINK_OFFER_CHECK_TOPIC, checkTime))
      true
    })


    LOG.warn("SmartLinkOfferSoldout handler done!")
  }

  case class smartlinkOffer(offerId: Int, createTime: String, countryIds: String, modes: String)


}

object bb{
  def main(args: Array[String]): Unit = {
//    println(",29,93,146,33,,147,".split(",").filter(x => !x.isEmpty).size == 5)
//    println(",1,2,3,4,5,".replace(",2,", ","))

//    println((CSTTime.timeFormatter.parse(CSTTime.now.time()).getSeconds - CSTTime.timeFormatter.parse("2017-10-20 19:21:56").getSeconds))// >= 24*8*3600)

//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    println((System.currentTimeMillis() - sdf.parse("2018-05-29 18:00:00").getTime())/3600/1000 >= 24*8)// >= 24*8*3600)
//

    /*val s = s"""
       |updata OFFER
       |set Modes = ,1,3,4,5,
       |where ID = 302051
     """.stripMargin

    println(s.split("where ID = ")(1))*/

//    println(",2,2".contains("2") && 1 == ",2,2".split(",").filter(x => !x.isEmpty).size)

    /*var reopenOfferSql = ""
    val oldModes = ",1,7,3,4,5,6,"
    if(! oldModes.contains(",2")){

      val tmp = oldModes.split(",").filter(x => !x.isEmpty).toList :+ "2"

      val modes = tmp.sorted.mkString(",", ",", ",")
      reopenOfferSql =
        s"""
           |UPDATE OFFER
           |SET Modes = $modes
           |
          """.stripMargin
    }

    println(reopenOfferSql)*/

    /*val l = List(123, 789, 267, 999, 888)
    val s = l.map(x => s"offerId = $x")
    .mkString("where( ", " or ", ")")

    println(s)*/

    val monthSplits = CSTTime.now.date().split("-")
    val b_date = monthSplits(0) + "-" + monthSplits(1) + "-01"
    println(b_date)
  }
}