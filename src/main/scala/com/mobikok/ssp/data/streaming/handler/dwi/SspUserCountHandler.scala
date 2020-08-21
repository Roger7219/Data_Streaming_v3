package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.SspUserIdHistory
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionRoolbackedCleanable}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

class SspUserCountHandler extends Handler {

  var userIdHistoryTable = "SSP_USERID_HISTORY"
  var userNewTopic = "topic_ad_user_new_v2"
  var userActiveTopic = "topic_ad_user_active_v2"
  val SSP_USER_COUNT_DROP_EXCEED_TABLE_CER = "sspUserCount_dropExceedTable_cer"
  val SSP_USER_COUNT_DROP_EXCEED_TABLE_TOPIC = "sspUserCount_dropExceedTable_topic"

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)
    userNewTopic = if(handlerConfig.hasPath("user.new.topic")) handlerConfig.getString("user.new.topic") else userNewTopic
    userActiveTopic = if(handlerConfig.hasPath("user.active.topic")) handlerConfig.getString("user.active.topic") else userActiveTopic
    userIdHistoryTable = if(handlerConfig.hasPath("uid.history.table")) handlerConfig.getString("uid.history.table") else userIdHistoryTable
  }

  override def doHandle (newDwi: DataFrame): DataFrame = {

    LOG.warn(s"SspUserCountHandler handle starting")

    /*newDwi.createOrReplaceTempView("SspUserCountHandler_newDwi")

    //去掉当前批次重复数据
    sql(
      s"""
         |select
         |  *,
         |  row_number() over(partition by userid order by 1) as num
         |from(
         |  select
         |    *,
         |    concat_ws('^', imei, cast(appId as string)) as userid
         |  from SspUserCountHandler_newDwi
         |)
         |where num = 1
         |
       """.stripMargin)*/

    val userIdHistoryTodayTable = s"${userIdHistoryTable}_${CSTTime.now.date()}"

    val userIdDf = newDwi
      .selectExpr(s"*", s"concat_ws('^', imei, cast(appId as string)) as userid")
      .dropDuplicates("userid")

    LOG.warn(s"userIdDf count(): ${userIdDf.count()}, take(2)", userIdDf.toJSON.take(2))

    val userIdDfT = "SspUserCountHandler_userIdDf"
    userIdDf.createOrReplaceTempView(userIdDfT)

    val ids = userIdDf.rdd.map(_.getAs[String]("userid")).collect()
    val res = hbaseClient.getsAsDF(userIdHistoryTable, ids, classOf[SspUserIdHistory])
    //创建天表，若不存在
    val s = hbaseClient.createTableIfNotExists(userIdHistoryTodayTable, userIdHistoryTable)
    if(s)
      LOG.warn("SspUserCountHandler", "createTableIfNotExists ", userIdHistoryTodayTable)
    val todayRes = hbaseClient.getsAsDF(userIdHistoryTodayTable, ids, classOf[SspUserIdHistory])

    LOG.warn("SspUserCountHandler", "hbaseRes take 4", res.toJSON.take(4), "count ", res.collect().length)
    LOG.warn("SspUserCountHandler", "hbaseTodayRes take 4", todayRes.toJSON.take(4), "count ", todayRes.collect().length)

    val activedT = "SspUserCountHandler_actived"
    val todayActivedT = "SspUserCountHandler_todayActived"
    res.createOrReplaceTempView(activedT)
    todayRes.createOrReplaceTempView(todayActivedT)

    //1. 新增（将activeTime值改为createTime值）
    val userNewSqls =
      if (res.collect().size > 0) s"select * from $userIdDfT u where not exists (select userid from $activedT where userid = u.userid)"
        else s"select * from $userIdDfT"

    val userNewDf = sql(userNewSqls)
      .withColumn("activeTime", expr("createTime"))

    //缓存df
    userNewDf.persist(StorageLevel.MEMORY_ONLY)


    //新增数据写入kafka新增用户topic
    if(userNewDf.collect().size > 0){
      val userNewMessages = userNewDf.drop("userid").toJSON.collect()

      LOG.warn("SspUserCountHandler", "userNewMessages take 4", userNewMessages.take(4).toList)
      kafkaClient.sendToKafka(userNewTopic, userNewMessages:_*)
    }


    //2. 活跃(activeTime 改为早期创建的时间(hbase中历史uid createTime))
//    sql("select * from userIdDf where userid in (select userid from actived)")
    var useractiveSqls = null.asInstanceOf[String]
    var activeUserDf = null.asInstanceOf[DataFrame]

    if (todayRes.collect().size > 0) {
      // 去除当天重复的活跃数据
      useractiveSqls =
        s"""
           |select
           |  t.*
           |from
           |  (select
           |    u.*,
           |    coalesce(a.createTime, u.createTime) as a_createTime
           |  from $userIdDfT u
           |  left join $activedT a on a.userid = u.userid
           |  ) t
           |where not exists (select userid from $todayActivedT where userid = t.userid)
         """.stripMargin
    }else{
      useractiveSqls =
        s"""
           |select
           |  u.*,
           |  coalesce(a.createTime, u.createTime) as a_createTime
           |from $userIdDfT u
           |left join $activedT a on a.userid = u.userid
         """.stripMargin
    }
      activeUserDf = sql(useractiveSqls)
        //      sql("select * from userIdDf u where exists (select userid from actived where userid = u.userid)")
        .withColumn("activeTime", expr("a_createTime"))
        .drop("a_createTime", "userid")


      //将活跃数据写入kafka活跃用户topic

      val userActiveMessages = activeUserDf.toJSON.collect()

      LOG.warn("SspUserCountHandler", "userActiveMessages take 4", userActiveMessages.take(4).toList)


      if (userActiveMessages.size > 0)
        kafkaClient.sendToKafka(userActiveTopic, userActiveMessages:_*)

      /*}else{
        useractiveSqls = "select * from userIdDf u where exists (select userid from actived where userid = u.userid)"//"select u.* from userIdDf u"

        activeUserDf = sql(useractiveSqls)
      }*/


    //将新增用户id和createTime写入hbase
    val newUids = userNewDf
      .selectExpr("userid", "createTime")
      .toJSON
      .rdd
      .map {x =>
        OM.toBean(x, classOf[SspUserIdHistory])
      }
      .collect()

    LOG.warn("SspUserCountHandler", "write_hbase take 4", newUids.take(4).toList)

    if(newUids.length > 0)
      hbaseClient.putsNonTransaction(userIdHistoryTable, newUids)

    // 为了去除当天重复的数据(将当前批次uid写入天表)
    val allUids = userIdDf
      .selectExpr("userid", "createTime")
      .toJSON
      .rdd
      .map {x =>
        OM.toBean(x, classOf[SspUserIdHistory])
      }
      .collect()

    LOG.warn("SspUserCountHandler", "allUids write_hbase take 4", allUids.take(4).toList)
    if (allUids.length > 0)
      hbaseClient.putsNonTransaction(userIdHistoryTodayTable, allUids)

    //清除缓存
    userNewDf.unpersist()

    //new user
   /* val userNewDf = userIdDf
        .filter { x =>

          val getRests = hbaseClient.gets[SspUserIdHistory](userIdHistoryTable, Array(x.getAs[String]("userid")) , classOf[SspUserIdHistory])

          x.getAs[String]("userid") != getRests.get(0).getUserid
        }


    val userNewData = userNewDf
        .drop("userid")
        .toJSON
        .collect()


    kafkaClient.sendToKafka(userNewTopic, userNewData:_*)


    //active user
    val userActiveData = userIdDf
      .map { x =>

        val getRests = hbaseClient.gets[SspUserIdHistory](userIdHistoryTable, Array(x.getAs[String]("userid")), classOf[SspUserIdHistory])

        (x, getRests.get(0) )
      }.filter{ x =>
        x._1.getAs[String]("userid") == x.getAs[String]("createTime")

      }
      .drop("userid")
      .toJSON
      .collect()

    kafkaClient.sendToKafka(userActiveTopic, userActiveData:_*)

    //write new userid to hbase

    val newUid = userNewDf
      .selectExpr("userid, createTime")
      .toJSON
      .rdd
      .map {x =>
        OM.toBean(x, classOf[SspUserIdHistory])
      }
      .collect()

    hbaseClient.putsNonTransaction(userIdHistoryTable, newUid)*/

    //凌晨一点异步删除，之前的天表
    val dropTime = CSTTime.now.modifyHourAsDate(-1)
    messageClient.pull(SSP_USER_COUNT_DROP_EXCEED_TABLE_CER, Array(SSP_USER_COUNT_DROP_EXCEED_TABLE_TOPIC), { x =>

      val toadyNeedDropTable =  x.isEmpty || ( !x.map(_.getKeyBody).contains(dropTime))

      if(toadyNeedDropTable){
        new Thread(new Runnable {
          override def run(): Unit = {

            val dropT = s"${userIdHistoryTable}_${CSTTime.now.modifyHourAsDate(-24)}"
            LOG.warn("SspUserCountHandler  drop exceed table start!", dropT)
            hbaseClient.deleteTable(dropT)

            LOG.warn("SspUserCountHandler drop exceed table end!")
            Thread.sleep(1000*10)

          }
        }).start()

        messageClient.push(new PushReq(SSP_USER_COUNT_DROP_EXCEED_TABLE_TOPIC, dropTime))
      }

      true
    })

    LOG.warn(s"SspUserCountHandler handle end!")
    newDwi

  }

  override def doCommit (): Unit = {
  }

  override def doClean (): Unit = {
  }
}