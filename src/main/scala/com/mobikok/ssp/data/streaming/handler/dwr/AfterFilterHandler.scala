package com.mobikok.ssp.data.streaming.handler.dwr

import com.mobikok.ssp.data.streaming.client.{ClickHouseClient, HBaseClient, HiveClient, TransactionManager}
import com.mobikok.ssp.data.streaming.util.Logger
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

@deprecated
class AfterFilterHandler extends Handler {

  var LOG: Logger = _
  var isMaster = false

  var dwrWhere: String = _

  override def init(moduleName: String, transactionManager: TransactionManager, hbaseClient: HBaseClient, hiveClient: HiveClient, clickHouseClient: ClickHouseClient, handlerConfig: Config, globalConfig: Config, expr: String, as: String): Unit = {
    super.init(moduleName, transactionManager, hbaseClient, hiveClient, clickHouseClient, handlerConfig, globalConfig, expr, as)
    LOG = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

    // 没有则抛出异常
    this.dwrWhere = handlerConfig.getString("where")
  }

  override def handle(unionedPersistenceDwr: DataFrame): DataFrame = {
    unionedPersistenceDwr.where(dwrWhere)
  }

}
