package com.mobikok.ssp.data.streaming.handler.dwi

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.TransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.entity.AggTrafficDWI
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
/**
  *
  * Created by Administrator on 2017/7/13.
  */
class JoinPhoenixHandler extends Handler {

  var unjoinDataTable = null.asInstanceOf[String]
  var phoenixTables = null.asInstanceOf[java.util.List[String]]

  @volatile var partentTid:String = null
  @volatile var subIncrTid:Int = 7


  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, expr: String, as: Array[String]): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, expr, as)

    unjoinDataTable = handlerConfig.getString("table.unjoin")

    phoenixTables = handlerConfig.getStringList("table.phoenix")
  }

  //persistenceDwr take(0): [2017-07-20,355364982160757,7486,1]
  override def handle (newDwi: DataFrame): (DataFrame, Array[TransactionCookie]) = {

    LOG.warn(s"JoinPhoenixHandler handle starting")

    if(partentTid == null) {
      partentTid = transactionManager.beginTransaction(moduleName)
    }

    var unj = hiveClient
      .hiveContext
      .read
      .table(unjoinDataTable)

//      unj.printSchema()
//      newDwi.selectExpr(
//      "null as repeats",
//      "null as rowkey",
//      "*",
//      "null as repeated",
//      "null as l_time",
//      "null as b_date"
//     ).printSchema()

    var res = unj
      .union(newDwi.selectExpr(
        "null as repeats",
        "null as rowkey",
        "*",
        "null as repeated",
        "null as l_time",
        "null as b_date"
      ))
      .alias("unj")

//    var res: DataFrame = hiveClient.hiveContext.createDataFrame(unj.collectAsList(), AggTrafficDWISchema.structType).alias("unj")

    phoenixTables.foreach{x=>
//      val r = hbaseClient.getsAsRDD(x, unj.select("clickId").rdd, classOf[AggTrafficDWI])
//      val df = hiveClient.hiveContext.createDataFrame(r).alias(x)

      val df = hbaseClient.getsAsDF(x, unj.select("clickId").rdd, classOf[AggTrafficDWI]).alias(x)

      LOG.warn(s"JoinPhoenixHandler read hbase table $x take(2)", df.take(2))

      val cs = as.map{y=>
        s"nvl(unj.$y, $x.$y) as $y"
      }
      res = res
        .join(df, col(s"$x.clickId") === col("unj.clickId"), "left_outer")
        .selectExpr(
          cs:_*
        ).alias("unj")

      //res = hiveClient.hiveContext.createDataFrame(res.collectAsList(), res.schema)

      LOG.warn(s"JoinPhoenixHandler join hbase table nvl() take(2)", res.take(2))
    }

//    val tid = partentTid + TransactionManager.parentTransactionIdSeparator + (subIncrTid)
    subIncrTid = subIncrTid + 1

    val c = hiveClient.overwrite(
//      tid,
      partentTid,
      unjoinDataTable,
      false,
      res
        .filter("appId is null")
        .selectExpr(
          s""" 0 as repeats""",
          s""" "" as rowkey""",
          s""" * """,
          s""" "N" as repeated """,
          s""" "2000-01-01" as l_time """,
          s""" to_date(reportTime) as b_date """
          ),
      "l_time", "b_date"
    )

//    hiveClient.commit(c)

    LOG.warn(s"JoinPhoenixHandler handle done")

    (res.filter("appId is not null"), Array(c))

  }

  override def init (): Unit = {}

  override def commit (cookie: TransactionCookie): Unit = {
    partentTid = null
    subIncrTid = 7
    hiveClient.commit(cookie)
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    hiveClient.rollback(cookies:_*)
  }

  override def clean (cookies: TransactionCookie*): Unit = {
    hiveClient.clean(cookies:_*)
  }
}



//
//s"nvl(last.appId, $x.appId) as appId",
//s"nvl(last.jarId, $x.jarId) as jarId",
//s"nvl(last.jarIds, $x.jarIds) as jarIds",
//s"nvl(last.publisherId, $x.publisherId) as publisherId",
//s"nvl(last.imei, $x.imei) as imei",
//
//s"nvl(last.imei, $x.imei) as imei",
//s"nvl(last.version, $x.version) as version",
//s"nvl(last.model, $x.model) as model",
//s"nvl(last.screen, $x.screen) as screen",
//s"nvl(last.installType, $x.installType) as installType",
//
//s"nvl(last.sv, $x.sv) as sv",
//s"nvl(last.leftSize, $x.leftSize) as leftSize",
//s"nvl(last.androidId, $x.androidId) as androidId",
//s"nvl(last.userAgent, $x.userAgent) as userAgent",
//s"nvl(last.connectType, $x.connectType) as connectType",
//
//s"nvl(last.createTime, $x.createTime) as createTime",
//s"nvl(last.clickTime, $x.clickTime) as clickTime",
//s"nvl(last.showTime, $x.showTime) as showTime",
//s"nvl(last.reportTime, $x.reportTime) as reportTime",
//s"nvl(last.countryId, $x.countryId) as countryId",
//
//s"nvl(last.carrierId, $x.carrierId) as carrierId",
//s"nvl(last.ipAddr, $x.ipAddr) as ipAddr",
//s"nvl(last.deviceType, $x.deviceType) as deviceType",
//s"nvl(last.pkgName, $x.pkgName) as pkgName",
//s"nvl(last.s1, $x.s1) as s1",
//
//s"nvl(last.s2, $x.s2) as s2",
//s"nvl(last.clickId, $x.clickId) as clickId",
//s"nvl(last.reportPrice, $x.reportPrice) as reportPrice",
//s"nvl(last.pos, $x.pos) as pos"