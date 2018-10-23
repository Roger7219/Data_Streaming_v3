package com.mobikok.ssp.data.streaming.handler.dwr

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.mobikok.ssp.data.streaming.entity.RowkeyUuid
import com.mobikok.ssp.data.streaming.exception.HandlerException
import com.mobikok.ssp.data.streaming.util.{Logger, MySqlJDBCClient}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{expr, _}
import org.apache.spark.sql.hive.HiveContext

/**
  *
  * Created by Administrator on 2017/7/13.
  */
class RowkeyUuidHandler extends Handler {

  var LOG: Logger = _

  var hiveContext: HiveContext = _
  var handlerConfig: Config = _
  var hbaseClient: HBaseClient = _
  var hbaseTable: String = _
  var exprStr:String = _
  var as: String = _

  override def init (moduleName: String, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config, exprStr: String, as: String): Unit = {

    LOG = new Logger(moduleName, getClass.getName, new Date().getTime)

    this.hbaseClient = hbaseClient
    this.hiveContext = hiveContext
    this.handlerConfig = handlerConfig
    this.hbaseTable = handlerConfig.getString("table")
    this.exprStr = exprStr
    this.as = as
  }

  //persistenceDwr take(0): [2017-07-20,355364982160757,7486,1]
  override def handle (persistenceDwr: DataFrame): DataFrame = {

    val rk = "rowkey"

    val rowkeyDwr = persistenceDwr
      .filter(expr(s"$exprStr is not null and trim($exprStr) <> '' "))
      .select(expr(exprStr).as(rk))
      .distinct()
      .alias("iud")

    val rowkeys = rowkeyDwr
      .rdd
      .map { x =>
        x.getAs[String](0)
      }
      .collect

    LOG.warn("RowkeyUuidHandler rowkeys take(5)", rowkeys.take(5))

    val ius = hbaseClient.getsAsDF(hbaseTable, rowkeys, classOf[RowkeyUuid]).alias("ius")

    var lastMaxUuid = ius.select(max("uuid")).first().getAs[Int](0)

    LOG.warn("RowkeyUuidHandler lastMaxUuid", lastMaxUuid)

    val r = rowkeyDwr
      .join(ius, col(s"iud.$rk") === col("ius.rowkey"), "left_outer")
      .selectExpr(
        s"iud.$rk",
        s"( nvl( ius.uuid, row_number() over(partition by 1 order by 1) ) + $lastMaxUuid ) as $as"
      )
      .alias("r")

    LOG.warn("RowkeyUuidHandler rowkeyDwr with generated uuid take(5)", r.take(5))

    //For spark serializable
    val _as = as
    val h = r
      .rdd
      .map { x =>
        RowkeyUuid(x.getAs[String](rk), x.getAs[Int](_as))//.setRowkey(Bytes.toBytes(x.getAs[String](rk)))
      }
      .collect()

    LOG.warn("RowkeyUuidHandler hbase will puts RowkeyUuid, count", h.length)

    hbaseClient.putsNonTransaction(hbaseTable, h)

    LOG.warn("RowkeyUuidHandler hbase puts RowkeyUuid completed, take(5)", h.take(5))

    val res = persistenceDwr
      .alias("d")
      .join(r, expr(s"d.$exprStr") === col(s"r.$rk"), "left_outer")
      .selectExpr(
        s"d.*",
        s"r.$as"
      )

    LOG.warn("RowkeyUuidHandler persistenceDwr joined uuid take(5)", res.take(5))

    res
  }

  override def prepare(dwi: DataFrame): DataFrame = dwi
}
