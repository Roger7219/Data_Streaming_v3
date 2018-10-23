package com.mobikok.ssp.data.streaming.handler.dm

import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.config.RDBConfig
import com.mobikok.ssp.data.streaming.util.{Logger, RunAgainIfError}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2017/7/13.
 */
trait Handler {

    var LOG: Logger = null
    var hbaseClient:HBaseClient = null
    var hiveContext: HiveContext = null
    var handlerConfig: Config = null
    var messageClient: MessageClient = null
    var kylinClientV2: KylinClientV2 = null
    var rDBConfig: RDBConfig = null
    var greenplumClient : GreenplumClient = null
    var bigQueryClient : BigQueryClient = null
    var clickHouseClient: ClickHouseClient = _

    var kafkaClient:KafkaClient = null
    def init(moduleName: String, bigQueryClient:BigQueryClient, greenplumClient:GreenplumClient, rDBConfig: RDBConfig, kafkaClient: KafkaClient, messageClient: MessageClient, kylinClientV2: KylinClientV2, hbaseClient: HBaseClient, hiveContext: HiveContext, handlerConfig: Config) = {
        LOG = new Logger(moduleName, getClass.getName, new Date().getTime)

        this.rDBConfig = rDBConfig
        this.hbaseClient = hbaseClient
        this.hiveContext = hiveContext
        this.handlerConfig = handlerConfig
        this.messageClient = messageClient
        this.kylinClientV2 = kylinClientV2
        this.kafkaClient = kafkaClient
        this.greenplumClient = greenplumClient
        this.bigQueryClient = bigQueryClient
    }

    def handle()

    def setClickHouseClient(clickHouseClient: ClickHouseClient): Unit = {
        this.clickHouseClient = clickHouseClient
    }

    def sql(sqlText: String): DataFrame ={
        var res:DataFrame = null
        RunAgainIfError.run({
            LOG.warn("Executing HQL", sqlText)
            res = hiveContext.sql(sqlText)
        })
        res
    }
}
