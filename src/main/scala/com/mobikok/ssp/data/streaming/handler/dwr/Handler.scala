package com.mobikok.ssp.data.streaming.handler.dwr

import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext;

/**
  * Created by Administrator on 2017/7/13.
  */
trait Handler {

  def init (moduleName: String,
            hbaseClient: HBaseClient,
            hiveContext: HiveContext,
            handlerConfig: Config,
            expr: String,
            as: String)

  def handle (persistenceDwr: DataFrame): DataFrame  // 返回新的groupby字段集

  def prepare(dwi: DataFrame): DataFrame

}
