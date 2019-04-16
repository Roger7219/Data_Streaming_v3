//package com.mobikok.ssp.data.streaming.client
//
//import java.sql.ResultSet
//import java.util
//import java.util.{Collections, Comparator, Date, Properties}
//
//import com.mobikok.ssp.data.streaming.client.cookie.{KafkaNonTransactionCookie, KafkaRollbackableTransactionCookie, TransactionCookie}
//import com.mobikok.ssp.data.streaming.entity.{OffsetRange, TopicPartition}
//import com.mobikok.ssp.data.streaming.exception.{KafkaClientException, MySQLJDBCClientException}
//import com.mobikok.ssp.data.streaming.module.support.TransactionalStrategy
//import com.mobikok.ssp.data.streaming.util.{KafkaOffsetTool, KafkaSender, Logger, MySqlJDBCClient}
//import com.mysql.jdbc.Driver
//import com.typesafe.config.{Config, ConfigException}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.log4j.Level
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//
//import scala.collection.JavaConversions._
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
///*
//CREATE TABLE `offset` (
//`topic` varchar(100) NOT NULL DEFAULT '',
//`partition` varchar(100) NOT NULL DEFAULT '',
//`offset` int(11) DEFAULT NULL,
//`module_name` varchar(255) NOT NULL DEFAULT '',
//`update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
//PRIMARY KEY (`topic`,`partition`,`module_name`)
//) ENGINE=InnoDB DEFAULT CHARSET=utf8
//*/
//
///**
//  * Created by Administrator on 2017/6/14.
//  */
//class KafkaClientV2 (moduleName: String, config: Config /*databaseUrl: String, user: String, password: String,*/ , transactionManager: MixTransactionManager, producerTopic: String) extends Transactional {
//
//  var bootstrapServers = config.getString("kafka.producer.set.bootstrap.servers")
//
//  var producerConfig = config.getConfig("kafka.producer.set").entrySet().map { x =>
//    x.getKey -> x.getValue.unwrapped.toString
//  }.toMap
//
//  var kafkaSender = new KafkaSender(producerTopic, producerTopic, Level.INFO, producerConfig);
//
//  def getLastOffsetAsJava (topics: Array[String]): java.util.Map[kafka.common.TopicAndPartition, java.lang.Long] = {
//    return KafkaOffsetTool.getInstance().getLatestOffset(bootstrapServers, topics.toList , "last_offsert_reader");
//  }
//
//  def getLatestOffset (topics: Array[String]): mutable.Map[kafka.common.TopicAndPartition, Long]  = {
//    return getLastOffsetAsJava(topics).map{x=> x._1 -> (x._2 + 0 )};
//  }
//
//  //  private[this] val LOG = Logger.getLogger(getClass().getName());
//  //var conn: Connection = null
//  var mySqlJDBCClient: MySqlJDBCClient = null
//
//  //  private val transactionalLegacyDataBackupTableSign = "_backup"
//
//  //mysql表名长度有限制
//  private val transactionalTmpTableSign = s"_m_${moduleName}_ts_" //s"_m_${moduleName}_trans_"
//  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_bc_" //s"_m_${moduleName}_backup_completed_"
//  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_bp_" //s"_m_${moduleName}_backup_progressing_"
//
//  private val backupCompletedTableNameForTransactionalTmpTableNameReplacePattern = transactionalLegacyDataBackupCompletedTableSign
//
//  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
//  var table: String = null
//
//  override def init (): Unit = {
//    LOG.warn(s"KafkaClient($moduleName) init started")
//    classOf[Driver]
//
//    mySqlJDBCClient = new MySqlJDBCClient(
//      config.getString(s"rdb.url"),
//      config.getString(s"rdb.user"),
//      config.getString(s"rdb.password")
//    )
//    //    conn = DriverManager.getConnection(
//    //      config.getString(s"rdb.url"),
//    //      config.getString(s"rdb.user"),
//    //      config.getString(s"rdb.password")
//    //    )
//    table = config.getString(s"rdb.kafka.offset.table")
//    //    createTableIfNotExists(table + transactionalLegacyDataBackupTableSign, table)
//    //    createTableIfNotExists(table + transactionalTmpTableSign, table)
//
//    LOG.warn(s"KafkaClient init completed")
//  }
//
//  def getCommitedOffset (topics: String*): Map[TopicPartition, Long] = {
//    val r = mySqlJDBCClient.executeQuery(
//      s"""select * from $table where """
//        + topics
//        .map { x =>
//          s""" module_name = "$moduleName" and topic = "$x" """
//        }
//        .mkString(" or ")
//    )
//    val b = new ArrayBuffer[(TopicPartition, Long)]
//    while (r.next()) {
//      b.append(new TopicPartition(r.getString("topic"), r.getInt("partition")) -> r.getLong("offset"))
//    }
//
//    try { r.close() }catch {case e:Exception=>}
//
//    b.toMap
//  }
//
//  //  def setOffset (tid: String, offsets: Map[TopicPartition, Long]): KafkaTransactionCookie = {
//  //    KafkaTransactionCookie(tid, offsets)
//  //  }
//
//  def setOffset (transactionParentId: String, offsetRanges: Array[OffsetRange]): TransactionCookie = {
//
//    var tid = transactionManager.generateTransactionId(transactionParentId)
//
//    if(transactionManager.needTransactionalAction()) {
//      return KafkaNonTransactionCookie(transactionParentId, tid)
//    }
//
//    val tt = table + transactionalTmpTableSign + tid
//    val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
//    val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid
//
//    val offsets = mapAsJavaMap(offsetRanges.map { x =>
//      new TopicPartition(x.topic, x.partition) -> x.untilOffset
//    }.toMap)
//
//    createTableIfNotExists(tt, table)
//
//    offsets.foreach { x =>
//      mySqlJDBCClient.execute(
//        s"""
//           | insert into $tt (
//           |   topic,
//           |   partition,
//           |   offset,
//           |   module_name,
//           |   update_time
//           | )
//           | values(
//           |   "${x._1.topic}",
//           |   "${x._1.partition}",
//           |   ${x._2},
//           |   "$moduleName",
//           |   now()
//           | )
//       """.stripMargin
//      )
//    }
//
//    new KafkaRollbackableTransactionCookie(
//      transactionParentId,
//      tid,
//      tt,
//      table,
//      pt,
//      ct,
//      offsets,
//      offsetRanges
//    )
//  }
//
//  def createTableIfNotExists (table: String, like: String): Unit = {
//    mySqlJDBCClient.execute(s"create table if not exists $table like $like")
//  }
//
//  override def commit (cookie: TransactionCookie): Unit = {
//
//    try {
//
//      if(cookie.isInstanceOf[KafkaNonTransactionCookie]) {
//        return
//      }
//
//      val c = cookie.asInstanceOf[KafkaRollbackableTransactionCookie]
//      //      val bt = table + transactionalLegacyDataBackupProgressingTableSign
//
//      //Back up legacy data for possible rollback
//      createTableIfNotExists(c.transactionalProgressingBackupTable, c.targetTable)
//
//      c.offsets.map { x =>
//        mySqlJDBCClient.execute(
//          s"""
//             | insert into ${c.transactionalProgressingBackupTable}
//             | select
//             |   topic,
//             |   partition,
//             |   offset,
//             |   module_name,
//             |   now() as update_time
//             | from $table
//             | where topic = "${x._1.topic}" and partition = "${x._1.partition}" and module_name = "$moduleName"
//         """.stripMargin)
//      }
//
//      mySqlJDBCClient.execute(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")
//
//      //Commit critical code
//      c.offsets.foreach { x =>
//        mySqlJDBCClient.execute(
//          s"""
//             | insert into $table (
//             |   topic,
//             |   partition,
//             |   offset,
//             |   module_name,
//             |   update_time
//             | )
//             | values(
//             |   "${x._1.topic}",
//             |   "${x._1.partition}",
//             |   ${x._2},
//             |   "$moduleName",
//             |   now()
//             | )
//             | on duplicate key update offset = values(offset)
//             | """.stripMargin
//        )
//      }
//    } catch {
//      case e: Exception =>
//        try {
//          init()
//        }
//        throw new KafkaClientException(s"Kafka Commit Offsets To RDBMS Fail, transactionId: " + cookie.id, e)
//    }
//
//  }
//
//  override def rollback (cookies: TransactionCookie*): Unit = {
//
//    try {
//
//      //      val bt = table + transactionalLegacyDataBackupTableSign
//
//      if (cookies.isEmpty) {
//        LOG.warn(s"KafkaClient rollback started(cookies is empty)")
//        //Revert to the legacy data!!
//        val brs = mySqlJDBCClient.executeQuery(s"""show tables like "%$transactionalLegacyDataBackupCompletedTableSign%" """)
//
//        val bts = new util.ArrayList[String]()
//        while (brs.next()) {
//          bts.add(brs.getString(1))
//        }
//        Collections.sort(bts, new Comparator[String] {
//          override def compare (a: String, b: String): Int = b.compareTo(a)
//        })
//
//        try { brs.close() }catch {case e:Exception=>}
//
//        bts.foreach { b =>
//
//          val parentTid = b
//            .split(transactionalLegacyDataBackupCompletedTableSign)(1)
//            .split(TransactionManager.parentTransactionIdSeparator)(0)
//
//          if (!transactionManager.isCommited(parentTid)) {
//            //var empty = true
//            //if (mySqlJDBCClient.executeQuery(s"select * from $x limit 1").next()) {
//            //  empty = false
//            //}
//            val empty = !mySqlJDBCClient.executeQuery(s"select * from $b limit 1").next()
//
//            val tt = b.replaceFirst(backupCompletedTableNameForTransactionalTmpTableNameReplacePattern, transactionalTmpTableSign)
//            if (empty) {
//              //Delete
//              val r = mySqlJDBCClient.executeQuery(s"select * from $tt")
//              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${tt}")
//              while (r.next()) {
//                mySqlJDBCClient.execute(
//                  s"""
//                     | delete from $table
//                     | where topic = "${r.getString("topic")}"
//                     | and partition = "${r.getString("partition")}"
//                     | and module_name = "$moduleName"
//                 """.stripMargin)
//              }
//
//              try { r.close() }catch {case e:Exception=>}
//
//            } else {
//              //Overwrite
//              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${b}")
//              mySqlJDBCClient.execute(
//                s"""
//                   | insert into $table
//                   | select *
//                   | from ${b}
//                   | on duplicate key update offset = values(offset)
//               """.stripMargin)
//            }
//          }
//
//        }
//
//        var x: ResultSet = mySqlJDBCClient.executeQuery(s"""show tables like "%${transactionalLegacyDataBackupCompletedTableSign}%" """)
//        while (x.next()) {
//          mySqlJDBCClient.execute(s"drop table ${x.getString(1)}")
//        }
//
//        x = mySqlJDBCClient.executeQuery(s"""show tables like "%${transactionalTmpTableSign}%" """)
//        while (x.next()) {
//          mySqlJDBCClient.execute(s"drop table ${x.getString(1)}")
//        }
//
//        try { x.close() }catch {case e:Exception=>}
//
//        return
//      }
//
//      LOG.warn(s"KafkaClient rollback started(cookies not empty)")
//      val cs = cookies.asInstanceOf[Array[KafkaRollbackableTransactionCookie]]
//
//      cs.foreach { x =>
//
//        x.offsets.foreach { y =>
//
//          val parentTid = x.id.split(TransactionManager.parentTransactionIdSeparator)(0)
//
//          if (!transactionManager.isCommited(parentTid)) {
//
//            val empty = !mySqlJDBCClient.executeQuery(s"select * from $x limit 1").next()
//            //Revert to the legacy data!!
//            if (empty) {
//              //Delete
//              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${x.transactionalTmpTable}")
//              val r = mySqlJDBCClient.executeQuery(s"select * from ${x.transactionalTmpTable}")
//              while (r.next()) {
//                mySqlJDBCClient.execute(
//                  s"""
//                     | delete from $table
//                     | where topic = "${r.getString("topic")}"
//                     | and partition = "${r.getString("partition")}"
//                     | and module_name = "$moduleName"
//                 """.stripMargin)
//              }
//
//            } else {
//              //Overwrite
//              LOG.warn(s"KafkaClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${x.transactionalCompletedBackupTable}")
//              mySqlJDBCClient.execute(
//                s"""
//                   | insert into $table
//                   | select *
//                   | from ${x.transactionalCompletedBackupTable}
//                   | on duplicate key update offset = values(offset)
//           """.stripMargin
//              )
//            }
//
//          }
//        }
//
//        x.offsets.foreach { y =>
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalCompletedBackupTable}")
//          //Delete it if exists
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalProgressingBackupTable}")
//          mySqlJDBCClient.execute(s"drop table if exists ${x.transactionalTmpTable}")
//
//        }
//      }
//      LOG.warn(s"KafkaClient($moduleName) rollback completed")
//
//    } catch {
//      case e: Exception =>
//        try {
//          init()
//        }
//        throw new MySQLJDBCClientException("Hive Transaction Rollback Fail, cookies: " + cookies, e)
//    }
//  }
//
//  override def clean (cookies: TransactionCookie*): Unit = {
//
//    cookies.foreach{x=>
//      if(x.isInstanceOf[KafkaRollbackableTransactionCookie]) {
//        var c = x.asInstanceOf[KafkaRollbackableTransactionCookie]
//        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalCompletedBackupTable}")
//        //Delete it if exists
//        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalProgressingBackupTable}")
//        mySqlJDBCClient.execute(s"drop table if exists ${c.transactionalTmpTable}")
//      }
//    }
//
//  }
//
//  // Create DirectStream
//  def createDirectStream[K, V] (topic: java.lang.String,
//                                config: Config,
//                                ssc: StreamingContext,
//                                module: String
//                               ): InputDStream[ConsumerRecord[K, V]] = {
//
//    createDirectStream(Array(topic), config, ssc, module)
//  }
//
//  def createDirectStream[K, V] (topics: scala.Iterable[java.lang.String],
//                                config: Config,
//                                ssc: StreamingContext,
//                                module: String
//                               ): InputDStream[ConsumerRecord[K, V]] = {
//
//    val kafkaParams = applyKafakConsumerParams(config, module)
//
//    KafkaUtils.createDirectStream[K, V](
//      ssc,
//      PreferConsistent,
//      Subscribe[K, V](topics, kafkaParams)
//    )
//  }
//
//  def createDirectStream[K, V] (_tp:org.apache.kafka.common.TopicPartition,
//                                config: Config,
//                                ssc: StreamingContext,
//                                topicPartitionOffsets: Map[TopicPartition, Long],
//                                module: String
//                               ): InputDStream[ConsumerRecord[K, V]] = {
//
//    val kafkaParams = applyKafakConsumerParams(config, module)
//
//
//    val tps = Array(_tp).asInstanceOf[Array[org.apache.kafka.common.TopicPartition]]  //getConfigTopicPartitions(module, config)
//
//    LOG.warn(
//      s"Creating DirectStream, Kafka Consumer Conf",
//      s"""
//         |Kafka Consumer Partitions Conf:\n${tps.map { x => s"topic: ${x.topic}, partition: ${x.partition}" }.mkString("\n")}\n
//         |Kafka Consumer Offsets Conf:\n${topicPartitionOffsets.mkString("\n")}\n
//         |Kafka Consumer Params:\n${kafkaParams.mkString("\n")}\n
//       """.stripMargin)
//
//    val _topicPartitionOffsets = topicPartitionOffsets.map { x =>
//      new org.apache.kafka.common.TopicPartition(x._1.topic, x._1.partition) -> x._2
//    }
//
//    KafkaUtils.createDirectStream[K, V](
//      ssc,
//      PreferConsistent,
//      Assign[K, V](tps /*topicPartitionOffsets.keys.toList*/ , kafkaParams, _topicPartitionOffsets)
//    )
//  }
//
//  def createDirectStream[K, V] (config: Config,
//                                ssc: StreamingContext,
//                                topicPartitionOffsets: Map[TopicPartition, Long],
//                                module: String
//                               ): InputDStream[ConsumerRecord[K, V]] = {
//
//    val kafkaParams = applyKafakConsumerParams(config, module)
//
//    val tp = getConfigTopicPartitions(module, config)
//
//    LOG.warn(
//      s"Creating DirectStream, Kafka Consumer Conf",
//      s"""
//         |Kafka Consumer Partitions Conf:\n${tp.map { x => s"topic: ${x.topic}, partition: ${x.partition}" }.mkString("\n")}\n
//         |Kafka Consumer Offsets Conf:\n${topicPartitionOffsets.mkString("\n")}\n
//         |Kafka Consumer Params:\n${kafkaParams.mkString("\n")}\n
//       """.stripMargin)
//
//    val _topicPartitionOffsets = topicPartitionOffsets.map { x =>
//      new org.apache.kafka.common.TopicPartition(x._1.topic, x._1.partition) -> x._2
//    }
//
//    KafkaUtils.createDirectStream[K, V](
//      ssc,
//      PreferConsistent,
////      ConsumerStrategies.Subscribe[K, V](tp.map{x=>x.topic()},kafkaParams)
//      Assign[K, V](tp /*topicPartitionOffsets.keys.toList*/ , kafkaParams, _topicPartitionOffsets)
//    )
//  }
//
//
//  def getConfigTopicPartitions (module: String, config: Config): Array[org.apache.kafka.common.TopicPartition] = {
//
//    val x = config.getConfigList(s"modules.$module.kafka.consumer.partitoins").map { x =>
//      new org.apache.kafka.common.TopicPartition(x.getString("topic"), x.getInt("partition"))
//    }
//    x.toArray
//    //(new Array[TopicPartition](1))
//    //.toArray[TopicPartition](new Array[TopicPartition](0))
//  }
//
//  private def applyKafakConsumerParams (config: Config, moduleName: String): scala.Predef.Map[String, Object] = {
//
//    //全局配置
//    var c = config.getConfig("kafka.consumer.set").entrySet().map { x =>
//      x.getKey -> x.getValue.unwrapped().toString
//    }.toMap
//
//    c += ("group.id" -> s"${moduleName}_group_id")
//    c += ("client.id" -> s"${moduleName}_client_id_${java.util.UUID.randomUUID()}")
//
//    //指定模块配置添加或覆盖全局配置项
//    try {
//      config.getConfig(s"modules.$moduleName.kafka.consumer.set")
//        .entrySet()
//        .foreach { x =>
//          c += (x.getKey -> x.getValue.unwrapped().toString)
//        }
//    } catch {
//      case e: ConfigException =>
//    }
//
//    c
//  }
//
////   Producer
//  def sendToKafka (topic: String, messages: String*): Unit = {
//    kafkaSender.send(messages:_*)
//  }
//
//  def sendToKafka (topic: String, message: String): Unit = {
//    kafkaSender.send(message)
//  }
//
//  def sendToKafka (topic: String, key: String, message: String): Unit = {
//    kafkaSender.send(key, message)
//  }
//
//}
