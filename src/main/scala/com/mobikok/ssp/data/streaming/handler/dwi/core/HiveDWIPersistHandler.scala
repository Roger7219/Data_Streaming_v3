package com.mobikok.ssp.data.streaming.handler.dwi.core

import com.mobikok.ssp.data.streaming.client._
import com.mobikok.ssp.data.streaming.client.cookie.HiveTransactionCookie
import com.mobikok.ssp.data.streaming.config.{ArgsConfig, RDBConfig}
import com.mobikok.ssp.data.streaming.handler.dwi.Handler
import com.mobikok.ssp.data.streaming.transaction.TransactionManager
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class HiveDWIPersistHandler extends Handler {

  val TTL_FOREVER = -1

  // 有配置表示enable
  var table: String = _
  var cookie: HiveTransactionCookie = _
  var ttl = TTL_FOREVER // 表数据保存时间,单位时天，过了这个时间将根据l_time删除过期的分区，-1表示永不删除

  override def init(moduleName: String, transactionManager: TransactionManager, rDBConfig: RDBConfig, hbaseClient: HBaseClient, hiveClient: HiveClient, kafkaClient: KafkaClient, argsConfig: ArgsConfig, handlerConfig: Config, globalConfig: Config, messageClient: MessageClient, moduleTracer: ModuleTracer): Unit = {
    super.init(moduleName, transactionManager, rDBConfig, hbaseClient, hiveClient, kafkaClient, argsConfig, handlerConfig, globalConfig, messageClient, moduleTracer)
    isAsynchronous = true
    table = globalConfig.getString(s"modules.$moduleName.dwi.table")
    ttl = if(globalConfig.hasPath(s"modules.$moduleName.dwi.ttl")) globalConfig.getInt(s"modules.$moduleName.dwi.ttl") else TTL_FOREVER
    tryCreateTTLCleanThread()
  }

  override def doHandle(newDwi: DataFrame): DataFrame = {
    val partitionFields = Array("repeated", "l_time", "b_date", "b_time"/*, "b_version"*/)

    if(isOverwriteFixedLTime) {
      cookie = hiveClient.overwrite(
        transactionManager.getCurrentTransactionParentId(),
        table,
        isOverwriteFixedLTime,
        newDwi,
        partitionFields.head,
        partitionFields.tail:_*)
    } else {
      cookie = hiveClient.into(
        transactionManager.getCurrentTransactionParentId(),
        table,
        newDwi,
        partitionFields.head,
        partitionFields.tail:_*)
    }

    transactionManager.collectTransactionCookie(hiveClient, cookie)

    LOG.warn("hiveClient write dwiTable completed", cookie)
    newDwi
  }

  override def doCommit(): Unit = {
    hiveClient.commit(cookie)

    // push message
    val dwiT = cookie
    var topic = dwiT.targetTable
    if (dwiT != null && dwiT.partitions != null && dwiT.partitions.nonEmpty) {
      val key = OM.toJOSN(dwiT.partitions.map { x => x.sortBy { y => y.name + y.value } }.sortBy { x => OM.toJOSN(x) })
      messageClient.push(PushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

      topic = moduleName
      messageClient.push(PushReq(topic, key))
      LOG.warn(s"MessageClient push done", s"topic: $topic, \nkey: $key")

    } else {
      LOG.warn(s"MessageClient dwi no hive partitions to push", s"topic: $topic")
    }
    cookie = null
  }

  override def doClean(): Unit = {
    transactionManager.cleanLastTransactionCookie(hiveClient)
  }

  def tryCreateTTLCleanThread(): Unit ={
    if(ttl > TTL_FOREVER) {
      new Thread(new Runnable {
        override def run(): Unit = {
          while(true){
            try{

              val expiredLTime = CSTTime.now.modifyHourAsTime(- 24 * ttl)

              hiveClient
                .partitions(table)
                .flatMap{x=>x}
                .filter{x=>"l_time".equals(x.name) && x.value < expiredLTime && !HiveClient.OVERWIRTE_FIXED_L_TIME.equals(x.value) && !"__HIVE_DEFAULT_PARTITION__".equals(x.value)  && StringUtil.notEmpty(x.value)  }
                .distinct
                .foreach{x=>
                  sql(s"alter table $table drop partition (l_time='${x.value}')")
                }

            }catch {
              case e: Throwable=> LOG.warn(s"Dwi table '$table' TTL clean thread error", e)
            }finally {
              try{
                Thread.sleep(1000*60*60*12) // 12小时执行一次
              }catch {
                case e: Throwable=> LOG.warn(s"Dwi table '$table' TTL clean thread error", e)
              }
            }
          }
        }
      }).start()
    }
  }
}








//    batchTransactionCookiesCache.add(cookie)
//  val batchTransactionCookiesCache = new util.ArrayList[TransactionCookie]()
//    var result = Array[TransactionCookie]()
//
//    val mixTransactionManager = transactionManager
//    if (mixTransactionManager.needRealTransactionalAction()) {
//      val needCleans = batchTransactionCookiesCache.filter(!_.parentId.equals(mixTransactionManager.getCurrentTransactionParentId()))
//      batchTransactionCookiesCache.removeAll(needCleans)
//      result = needCleans.toArray
//    }
//    hiveClient.clean(result:_*)