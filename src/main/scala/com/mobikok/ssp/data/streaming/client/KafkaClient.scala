package com.mobikok.ssp.data.streaming.client

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.{Collections, Comparator, Date, Properties}

import com.mobikok.ssp.data.streaming.entity.{OffsetRange, TopicPartition}
import com.mobikok.ssp.data.streaming.exception.{HiveClientException, KafkaClientException, MySQLJDBCClientException}
import com.mobikok.ssp.data.streaming.client.cookie.{HiveRollbackableTransactionCookie, KafkaNonTransactionCookie, KafkaRollbackableTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.module.support.TransactionalStrategy
import com.mobikok.ssp.data.streaming.util.{KafkaOffsetTool, KafkaSender, Logger, MySqlJDBCClient}
import com.mysql.jdbc.Driver
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}
/*
CREATE TABLE `offset` (
`topic` varchar(100) NOT NULL DEFAULT '',
`partition` varchar(100) NOT NULL DEFAULT '',
`offset` int(11) DEFAULT NULL,
`module_name` varchar(255) NOT NULL DEFAULT '',
`update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`topic`,`partition`,`module_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/

/**
  * Created by Administrator on 2017/6/14.
  */
class KafkaClient (moduleName: String, config: Config /*databaseUrl: String, user: String, password: String,*/ , transactionManager: MixTransactionManager) extends Transactional {

  def getLastOffsetAsJava (topics: Array[String]): java.util.Map[kafka.common.TopicAndPartition, java.lang.Long] = {
    return KafkaOffsetTool.getLatestOffset(consumerBootstrapServers, topics.toList , "last_offsert_reader");
  }

  def getLatestOffset(topics: Array[String]): mutable.Map[kafka.common.TopicAndPartition, Long]  = {
    return getLastOffsetAsJava(topics).map{x=> x._1 -> (x._2 + 0 )};
  }

  def getEarliestOffsetAsJava (topics: Array[String]): java.util.Map[kafka.common.TopicAndPartition, java.lang.Long] = {
    return KafkaOffsetTool.getEarliestOffset(consumerBootstrapServers, topics.toList , "earliest_offsert_reader");
  }

  def getEarliestOffset (topics: Array[String]): mutable.Map[kafka.common.TopicAndPartition, Long]  = {
    return getEarliestOffsetAsJava(topics).map{x=> x._1 -> (x._2 + 0 )};
  }

  //  private[this] val LOG = Logger.getLogger(getClass().getName());
  //var conn: Connection = null
  var mySqlJDBCClient: MySqlJDBCClient = null

  //  private val transactionalLegacyDataBackupTableSign = "_backup"

  //mysql表名长度有限制
  private val transactionalTmpTableSign = s"_m_${moduleName}_ts_" //s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_bc_" //s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_bp_" //s"_m_${moduleName}_backup_progressing_"

  private val backupCompletedTableNameForTransactionalTmpTableNameReplacePattern = transactionalLegacyDataBackupCompletedTableSign

  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
  var table: String = null

  override def init (): Unit = {
    LOG.warn(s"KafkaClient init started")
    classOf[Driver]

    mySqlJDBCClient = new MySqlJDBCClient(
      config.getString(s"rdb.url"),
      config.getString(s"rdb.user"),
      config.getString(s"rdb.password")
    )
    //    conn = DriverManager.getConnection(
    //      config.getString(s"rdb.url"),
    //      config.getString(s"rdb.user"),
    //      config.getString(s"rdb.password")
    //    )
    table = config.getString(s"rdb.kafka.offset.table")
    //    createTableIfNotExists(table + transactionalLegacyDataBackupTableSign, table)
    //    createTableIfNotExists(table + transactionalTmpTableSign, table)

    LOG.warn(s"KafkaClient init completed")
  }

  def getCommitedOffset (topics: String*): Map[TopicPartition, Long] = {
    val r = mySqlJDBCClient.executeQuery(
      s"""select * from $table where """
        + topics
        .map { x =>
          s""" module_name = "$moduleName" and topic = "$x" """
        }
        .mkString(" or ")
    )
    val b = new ArrayBuffer[(TopicPartition, Long)]
    while (r.next()) {
      b.append(new TopicPartition(r.getString("topic"), r.getInt("partition")) -> r.getLong("offset"))
    }

    try { r.close() }catch {case e:Exception=>}

    b.toMap
  }

  //  def setOffset (tid: String, offsets: Map[TopicPartition, Long]): KafkaTransactionCookie = {
  //    KafkaTransactionCookie(tid, offsets)
  //  }

  def setOffset (transactionParentId: String, offsetRanges: Array[OffsetRange]): TransactionCookie = {

    val tid = transactionManager.generateTransactionId(transactionParentId)

    if(!transactionManager.needTransactionalAction()) {
      offsetRanges.foreach { x =>
        mySqlJDBCClient.execute(
          s"""
             | insert into $table (
             |   topic,
             |   partition,
             |   offset,
             |   module_name,
             |   update_time
             | )
             | values(
             |   "${x.topic}",
             |   "${x.partition}",
             |   ${x.untilOffset},
             |   "$moduleName",
             |   now()
             | )
             | on duplicate key update offset = values(offset)
             | """.stripMargin
        )
      }

      return KafkaNonTransactionCookie(transactionParentId, tid)
    }

    val tt = table + transactionalTmpTableSign + tid
    val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
    val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

    val offsets = mapAsJavaMap(offsetRanges.map { x =>
      new TopicPartition(x.topic, x.partition) -> x.untilOffset
    }.toMap)

    createTableIfNotExists(tt, table)

    offsets.foreach { x =>
      mySqlJDBCClient.execute(
        s"""
           | insert into $tt (
           |   topic,
           |   partition,
           |   offset,
           |   module_name,
           |   update_time
           | )
           | values(
           |   "${x._1.topic}",
           |   "${x._1.partition}",
           |   ${x._2},
           |   "$moduleName",
           |   now()
           | )
       """.stripMargin
      )
    }

    new KafkaRollbackableTransactionCookie(
      transactionParentId,
      tid,
      tt,
      table,
      pt,
      ct,
      offsets,
      offsetRanges
    )
  }

  def setOffset(offsets: mutable.Map[kafka.common.TopicAndPartition, Long]) {
    offsets.foreach { x =>
      mySqlJDBCClient.execute(
        s"""
           | insert into offset (
           |   topic,
           |   partition,
           |   offset,
           |   module_name,
           |   update_time
           | )
           | values(
           |   "${x._1.topic}",
           |   "${x._1.partition}",
           |   ${x._2},
           |   "$moduleName",
           |   now()
           | )
           | on duplicate key update offset = values(offset)
       """.stripMargin
      )
    }
  }

  def createTableIfNotExists (table: String, like: String): Unit = {
    if(table.length > 64) {
      throw new KafkaClientException(s"The table name '$table' cannot exceed 64 characters in length.")
    }
    mySqlJDBCClient.execute(s"create table if not exists $table like $like")
  }

  override def commit (cookie: TransactionCookie): Unit = {

    try {

      if(cookie.isInstanceOf[KafkaNonTransactionCookie]) {
        return
      }

      val c = cookie.asInstanceOf[KafkaRollbackableTransactionCookie]
      //      val bt = table + transactionalLegacyDataBackupProgressingTableSign

      //Back up legacy data for possible rollback
      createTableIfNotExists(c.transactionalProgressingBackupTable, c.targetTable)

      c.offsets.map { x =>
        mySqlJDBCClient.execute(
          s"""
             | insert into ${c.transactionalProgressingBackupTable}
             | select
             |   topic,
             |   partition,
             |   offset,
             |   module_name,
             |   now() as update_time
             | from $table
             | where topic = "${x._1.topic}" and partition = "${x._1.partition}" and module_name = "$moduleName"
         """.stripMargin)
      }

      mySqlJDBCClient.execute(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")

      //Commit critical code
      c.offsets.foreach { x =>
        mySqlJDBCClient.execute(
          s"""
             | insert into $table (
             |   topic,
             |   partition,
             |   offset,
             |   module_name,
             |   update_time
             | )
             | values(
             |   "${x._1.topic}",
             |   "${x._1.partition}",
             |   ${x._2},
             |   "$moduleName",
             |   now()
             | )
             | on duplicate key update offset = values(offset)
             | """.stripMargin
        )
      }
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new KafkaClientException(s"Kafka Commit Offsets To RDBMS Fail, transactionId: " + cookie.id, e)
    }

  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {

    try {
      val cleanable = new Cleanable()
      //      val bt = table + transactionalLegacyDataBackupTableSign

      if (cookies.isEmpty) {
        LOG.warn(s"KafkaClient rollback started(cookies is empty)")
        //Revert to the legacy data!!
        val brs = mySqlJDBCClient.executeQuery(s"""show tables like "%$transactionalLegacyDataBackupCompletedTableSign%" """)

        val bts = new util.ArrayList[String]()
        while (brs.next()) {
          bts.add(brs.getString(1))
        }
        Collections.sort(bts, new Comparator[String] {
          override def compare (a: String, b: String): Int = b.compareTo(a)
        })

        try { brs.close() }catch {case e:Exception=>}

        bts.foreach { b =>

          val parentTid = b
            .split(transactionalLegacyDataBackupCompletedTableSign)(1)
            .split(TransactionManager.parentTransactionIdSeparator)(0)

          //Key code !!
          val needRollback = transactionManager.isActiveButNotAllCommited(parentTid)

          if (needRollback /*!transactionManager.isCommited(parentTid)*/) {
            //var empty = true
            //if (mySqlJDBCClient.executeQuery(s"select * from $x limit 1").next()) {
            //  empty = false
            //}
            val empty = !mySqlJDBCClient.executeQuery(s"select * from $b limit 1").next()

            val tt = b.replaceFirst(backupCompletedTableNameForTransactionalTmpTableNameReplacePattern, transactionalTmpTableSign)
            if (empty) {
              //Delete
              val r = mySqlJDBCClient.executeQuery(s"select * from $tt")
              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${tt}")
              while (r.next()) {
                mySqlJDBCClient.execute(
                  s"""
                     | delete from $table
                     | where topic = "${r.getString("topic")}"
                     | and partition = "${r.getString("partition")}"
                     | and module_name = "$moduleName"
                 """.stripMargin)
              }

              try { r.close() }catch {case e:Exception=>}

            } else {
              //Overwrite
              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${b}")
              mySqlJDBCClient.execute(
                s"""
                   | insert into $table
                   | select *
                   | from ${b}
                   | on duplicate key update offset = values(offset)
               """.stripMargin)
            }
          }

        }
// fix bug
        var x: ResultSet = mySqlJDBCClient.executeQuery(s"""show tables like "%${transactionalLegacyDataBackupCompletedTableSign}%" """)
        while (x.next()) {
          val t = x.getString(1)
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table if exists ${t}")}
//          mySqlJDBCClient.execute(s"drop table ${x.getString(1)}")
        }

        x = mySqlJDBCClient.executeQuery(s"""show tables like "%${transactionalLegacyDataBackupProgressingTableSign}%" """)
        while (x.next()) {
          val t = x.getString(1)
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table ${t}")}
          //          mySqlJDBCClient.execute(s"drop table ${x.getString(1)}")
        }

        x = mySqlJDBCClient.executeQuery(s"""show tables like "%${transactionalTmpTableSign}%" """)
        while (x.next()) {
          val t = x.getString(1)
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table if exists ${t}")}
//          mySqlJDBCClient.execute(s"drop table ${x.getString(1)}")
        }

        try { x.close() }catch {case e:Exception=>}

        return cleanable
      }

      LOG.warn(s"KafkaClient rollback started(cookies not empty)")
      val cs = cookies.asInstanceOf[Array[KafkaRollbackableTransactionCookie]]

      cs.foreach { x =>

        x.offsets.foreach { y =>

          val parentTid = x.id.split(TransactionManager.parentTransactionIdSeparator)(0)

          //Key code !!
          val needRollback = transactionManager.isActiveButNotAllCommited(parentTid)

          if (needRollback/*!transactionManager.isCommited(parentTid)*/) {

            val empty = !mySqlJDBCClient.executeQuery(s"select * from $x limit 1").next()
            //Revert to the legacy data!!
            if (empty) {
              //Delete
              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${x.transactionalTmpTable}")
              val r = mySqlJDBCClient.executeQuery(s"select * from ${x.transactionalTmpTable}")
              while (r.next()) {
                mySqlJDBCClient.execute(
                  s"""
                     | delete from $table
                     | where topic = "${r.getString("topic")}"
                     | and partition = "${r.getString("partition")}"
                     | and module_name = "$moduleName"
                 """.stripMargin)
              }

            } else {
              //Overwrite
              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${x.transactionalCompletedBackupTable}")
              mySqlJDBCClient.execute(
                s"""
                   | insert into $table
                   | select *
                   | from ${x.transactionalCompletedBackupTable}
                   | on duplicate key update offset = values(offset)
           """.stripMargin
              )
            }

          }
        }

        x.offsets.foreach { y =>
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalCompletedBackupTable}")}
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalProgressingBackupTable}")}
          cleanable.addAction{mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalTmpTable}")}
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalCompletedBackupTable}")
//          //Delete it if exists
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalProgressingBackupTable}")
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalTmpTable}")
        }
      }
      LOG.warn(s"KafkaClient($moduleName) rollback completed")
      cleanable
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new MySQLJDBCClientException("Hive Transaction Rollback Fail, cookies: " + cookies, e)
    }
  }

  override def clean (cookies: TransactionCookie*): Unit = {

    cookies.foreach{x=>
      if(x.isInstanceOf[KafkaRollbackableTransactionCookie]) {
        var c = x.asInstanceOf[KafkaRollbackableTransactionCookie]
        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalCompletedBackupTable}")
        //Delete it if exists
        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalProgressingBackupTable}")
        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalTmpTable}")
      }
    }

  }

  // Create DirectStream
  def createDirectStream[K, V] (topic: java.lang.String,
                                config: Config,
                                ssc: StreamingContext,
                                module: String
                               ): InputDStream[ConsumerRecord[K, V]] = {

    createDirectStream(Array(topic), config, ssc, module)
  }

  def createDirectStream[K, V] (topics: scala.Iterable[java.lang.String],
                                config: Config,
                                ssc: StreamingContext,
                                module: String
                               ): InputDStream[ConsumerRecord[K, V]] = {

    val kafkaParams = applyKafakConsumerParams(config, module)

    KafkaUtils.createDirectStream[K, V](
      ssc,
      PreferConsistent,
      Subscribe[K, V](topics, kafkaParams)
    )
  }

  def createDirectStream[K, V] (_tp:org.apache.kafka.common.TopicPartition,
                                config: Config,
                                ssc: StreamingContext,
                                topicPartitionOffsets: Map[TopicPartition, Long],
                                module: String
                               ): InputDStream[ConsumerRecord[K, V]] = {

    val kafkaParams = applyKafakConsumerParams(config, module)


    val tps = Array(_tp).asInstanceOf[Array[org.apache.kafka.common.TopicPartition]]  //getConfigTopicPartitions(module, config)

    LOG.warn(
      s"Creating DirectStream, Kafka Consumer Conf",
      s"""
         |Kafka Consumer Partitions Conf:\n${tps.map { x => s"topic: ${x.topic}, partition: ${x.partition}" }.mkString("\n")}\n
         |Kafka Consumer Offsets Conf:\n${topicPartitionOffsets.mkString("\n")}\n
         |Kafka Consumer Params:\n${kafkaParams.mkString("\n")}\n
       """.stripMargin)

    val _topicPartitionOffsets = topicPartitionOffsets.map { x =>
      new org.apache.kafka.common.TopicPartition(x._1.topic, x._1.partition) -> x._2
    }

    KafkaUtils.createDirectStream[K, V](
      ssc,
      PreferConsistent,
      Assign[K, V](tps /*topicPartitionOffsets.keys.toList*/ , kafkaParams, _topicPartitionOffsets)
    )
  }

  def createDirectStream[K, V] (config: Config,
                                ssc: StreamingContext,
                                topicPartitionOffsets: Map[TopicPartition, Long],
                                module: String
                               ): InputDStream[ConsumerRecord[K, V]] = {

    val kafkaParams = applyKafakConsumerParams(config, module)

    //*.config文件配置的分区
    val tp = getConfigTopicPartitions(module, config)

    LOG.warn(
      s"Creating DirectStream, Kafka Consumer Conf",
      s"""
         |Kafka Consumer Partitions File Conf:\n${tp.map { x => s"topic: ${x.topic}, partition: ${x.partition}" }.mkString("\n")}\n
         |Kafka Consumer Offsets BD Conf:\n${topicPartitionOffsets.mkString("\n")}\n
         |Kafka Consumer Params:\n${kafkaParams.mkString("\n")}\n
       """.stripMargin)

    val ps = tp.map(_.partition())
    val _topicPartitionOffsets = topicPartitionOffsets.filter{x=> ps.contains(x._1.partition)}.map { x =>
      new org.apache.kafka.common.TopicPartition(x._1.topic, x._1.partition) -> x._2
    }

    KafkaUtils.createDirectStream[K, V](
      ssc,
      PreferConsistent,
//      ConsumerStrategies.Subscribe[K, V](tp.map{x=>x.topic()},kafkaParams)
      Assign[K, V](tp /*topicPartitionOffsets.keys.toList*/ , kafkaParams, _topicPartitionOffsets)
    )
  }


  def getConfigTopicPartitions (module: String, config: Config): Array[org.apache.kafka.common.TopicPartition] = {

    val x = config.getConfigList(s"modules.$module.kafka.consumer.partitions").map { x =>
      new org.apache.kafka.common.TopicPartition(x.getString("topic"), x.getInt("partition"))
    }
    x.toArray
    //(new Array[TopicPartition](1))
    //.toArray[TopicPartition](new Array[TopicPartition](0))
  }

  private def applyKafakConsumerParams (config: Config, moduleName: String): scala.Predef.Map[String, Object] = {

    //全局配置
    var c = config.getConfig("kafka.consumer.set").entrySet().map { x =>
      x.getKey -> x.getValue.unwrapped().toString
    }.toMap

    c += ("group.id" -> s"${moduleName}_group_id")
    c += ("client.id" -> s"${moduleName}_client_id_${java.util.UUID.randomUUID()}")

    //指定模块配置添加或覆盖全局配置项
    try {
      config.getConfig(s"modules.$moduleName.kafka.consumer.set")
        .entrySet()
        .foreach { x =>
          c += (x.getKey -> x.getValue.unwrapped().toString)
        }
    } catch {
      case e: ConfigException =>
    }

    c
  }


  // Producer
  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerConfig
  import org.apache.kafka.clients.producer.ProducerRecord
  import org.apache.kafka.clients.producer.RecordMetadata

  var producerIsAsync: Boolean = false
  try {
    producerIsAsync = config.getBoolean("kafka.producer.is.async")
  } catch {
    case e: Exception =>
  }

  var producerBootstrapServers: String = null
  var consumerBootstrapServers: String = null
  try {
    producerBootstrapServers = config.getString("kafka.producer.set.bootstrap.servers")
  } catch {
    case e: Exception =>
  }
  try {
    consumerBootstrapServers = config.getString("kafka.consumer.set.bootstrap.servers")
  } catch {
    case e: Exception =>
  }

  var producerClientId: String = null
  try {
    producerClientId = config.getString("kafka.producer.set.client.id")
  } catch {
    case e: Exception =>
  }

  var producerAcks: String = null
  try {
    producerAcks = config.getString("kafka.producer.set.acks")
  } catch {
    case e: Exception =>
  }

  var producerKeySerializer: String = null
  try {
    producerKeySerializer = config.getString("kafka.producer.set.key.serializer")
  } catch {
    case e: Exception =>
  }

  var producerValueSerializer: String = null
  try {
    producerValueSerializer = config.getString("kafka.producer.set.value.serializer")
  } catch {
    case e: Exception =>
  }

  var producerRetries = "0" //Int.MaxValue.toString

  private var isClose = false
  private var producer: KafkaProducer[String, String] = null

  def createIfInvalidProducer (): Unit = {
    if (producer == null || isClose) {
      producer = new KafkaProducer[String, String](getProducerConfig)
    }
  }

  @deprecated
  def sendToKafka (topic: String, messages: String*): Unit = {
    messages.foreach { x =>
      sendToKafka(topic, null, x, true)
    }
    producer.flush()
  }
  @deprecated
  def sendToKafka (topic: String, message: String): Unit = {
    sendToKafka(topic, null, message)
  }
  @deprecated
  def sendToKafka (topic: String, key: String, message: String): Unit = {
    sendToKafka(topic, key, message, producerIsAsync)
  }

  @deprecated
  //defaultProducerIsAsync: 是否异步发送，配置文件里配置了默认值
  def sendToKafka (topic: String, key: String, message: String, defaultProducerIsAsync: Boolean): Unit = {
    try {
      createIfInvalidProducer()
      if (defaultProducerIsAsync) {
        producer.send(new ProducerRecord[String, String](topic, key, message), new MessageProduceCallback(System.currentTimeMillis, key, message))
      }
      else {
        producer.send(new ProducerRecord[String, String](topic, key, message)).get
      }
    } catch {
      case e: Exception =>
        LOG.error("Exception when sending Kafka messages!!!", e)
        producer.close()
        throw e
    }
  }

  def closeProducer (): Unit = {
    producer.close()
    isClose = true
  }

  private def getProducerConfig = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerBootstrapServers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId + "-" + new Date().getTime)
    props.put(ProducerConfig.ACKS_CONFIG, "-1"/*producerAcks*/)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerKeySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerValueSerializer)
//    props.put(ProducerConfig.RETRIES_CONFIG, producerRetries)

    props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Int.MaxValue))
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MaxValue))
    props.put("block.on.buffer.full", String.valueOf(true))
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(50))
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(1000 * 60 * 10) )

    props
  }

  class MessageProduceCallback (val startTime: Long, val key: String, val msg: String) extends org.apache.kafka.clients.producer.Callback {
    override def onCompletion (recordMetadata: RecordMetadata, e: Exception): Unit = {
      val elapsedTime = System.currentTimeMillis - startTime
      if (recordMetadata != null) {
        LOG.trace("Sended: " + msg + " be sended to partition no : " + recordMetadata.partition + ", elapsedTime: " + elapsedTime + "ms")
      } else {
        LOG.trace("Sended" + msg + " be sended,  recordMetadata is null")
      }
    }
  }


}
