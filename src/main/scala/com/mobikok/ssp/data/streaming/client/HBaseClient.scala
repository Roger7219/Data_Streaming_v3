package com.mobikok.ssp.data.streaming.client

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.regex.Pattern

import com.mobikok.ssp.data.streaming.App.config
import com.mobikok.ssp.data.streaming.exception.HBaseClientException
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.util.{CSTTime, Logger, ModuleTracer, OM}
import com.typesafe.config.Config
import org.apache.derby.iapi.sql.dictionary.TableDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import org.apache.hadoop.hbase.util._

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.math.Ordering

/**
  * 暂时只支持单客户端，多可客户端有并发问题
  * Created by Administrator on 2017/6/8.
  */
class HBaseClient (moduleName: String, sc: SparkContext, config: Config, transactionManager: MixTransactionManager, moduleTracer: ModuleTracer) extends Transactional {
  //  private[this] val LOG = Logger.getLogger(getClass().getName())
  private val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  private var hiveContext: HiveContext = null;

  private var directHBaseConf: Configuration = null
  private var directHBaseConnection: Connection = null
  private var directHBaseAdmin: Admin = null

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"

  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"

  private val backupCompletedTableNameWithinClassNamePattern = ("_class_(.+)" + transactionalLegacyDataBackupCompletedTableSign).r

  private val backupCompletedTableNameForTransactionalTmpTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign
  private val backupCompletedTableNameForTransactionalTargetTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign + ".*"

  override def init (): Unit = {
    LOG.warn(s"HBaseClient init started")
    hiveContext = new HiveContext(sc)
    directHBaseConf = HBaseConfiguration.create
    config
      .getConfig("hbase.set")
      .entrySet()
      .foreach { x =>
        directHBaseConf.set(x.getKey, x.getValue.unwrapped().toString)
      }

    directHBaseConnection = ConnectionFactory.createConnection(directHBaseConf)
    directHBaseAdmin = directHBaseConnection.getAdmin

    //    config.getStringList("hbase.transactional.tables").foreach { x =>
    //      if (!generalAdmin.isTableAvailable(TableName.valueOf(s"$x$transactionalTmpTableSuffix"))) {
    //
    //        val like = generalAdmin.getTableDescriptor(TableName.valueOf(x))
    //
    //        val desc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(s"$x$transactionalTmpTableSuffix"))
    //
    //        like.getFamilies.foreach { x =>
    //          desc.addFamily(x)
    //        }
    //        generalAdmin.createTable(desc)
    //      }
    //    }
    LOG.warn(s"HBaseClient init completed")

  }

  def gets[T <: HBaseStorable] (table: String,
                                rowkeys: Array[_ <: Object],
                                hbaseStorableClass: Class[T]
                               ): util.ArrayList[T] = {
    var result: util.ArrayList[T] = null
    var b = true
    while (b)
    try {
      result = gets0(table, rowkeys, hbaseStorableClass)
      b = false
    }catch {case e:Exception=>
      LOG.warn("HBase gets fail, Will try again !!", e)
      Thread.sleep(5000)
    }

    result
  }

  private def gets0[T <: HBaseStorable] (table: String,
                                         rowkeys: Array[_ <: Object],
                                         hbaseStorableClass: Class[T]
                                        ): util.ArrayList[T] = {

    moduleTracer.trace("    hbase gets0 start")
    LOG.warn("hbase gets0 start", s"table: $table\nrowkeys count: ${rowkeys.length}")

    val t = directHBaseConnection.getTable(TableName.valueOf(table))

    val gets = new util.ArrayList[Get](rowkeys.size)

    rowkeys.foreach { x =>

      if (x.isInstanceOf[Array[Byte]]) {
        gets.add(new Get(x.asInstanceOf[Array[Byte]]))
      }
      else if (x.isInstanceOf[String]) {
        gets.add(new Get(Bytes.toBytes(x.asInstanceOf[String])))
      } else if (x.isInstanceOf[Int]) {
        gets.add(new Get(Bytes.toBytes(x.asInstanceOf[Int])))
      } else if (x.isInstanceOf[Long]) {
        gets.add(new Get(Bytes.toBytes(x.asInstanceOf[Long])))
      } else {
        throw new HBaseClientException(s"Rowkey type '${x.getClass.getName}' is not supported when batch get operating")
      }
    }

    val r = new util.ArrayList[T](rowkeys.size)
    t.get(gets).foreach { x =>

      if (!x.isEmpty) r.add(HBaseClient.assemble(x, hbaseStorableClass))
    }
    t.close()


    moduleTracer.trace("    hbase gets0 done")
    LOG.warn("hbase gets0 done, result count", r.size)

    r
  }

  def getsAsDF[T <: HBaseStorable] (table: String,
                                    ids: Array[_ <: Object],
                                    hbaseStorableClass: Class[T]
                                   ): DataFrame = {

    LOG.warn("hbase table getsAsDF start", "table: "+ table + "\nclazz: " +hbaseStorableClass.getName)

    val r = gets0(table, ids, hbaseStorableClass)

    if (r.isEmpty) {
      val e = hiveContext.sparkContext.emptyRDD[org.apache.spark.sql.Row]
      return hiveContext.createDataFrame(e, hbaseStorableClass.newInstance().structType)
    }

    val res = hiveContext.createDataFrame(
      r.map { x =>
        x.toSparkRow()
      },
      hbaseStorableClass.newInstance().structType
    )

    moduleTracer.trace("    hbase getsAsDF done")
    LOG.warn("hbase table getsAsDF done")

    res
  }

  def getsAsDF[T <: HBaseStorable] (table: String,
                                     rowkeys: RDD[_ <: Object],
                                     hbaseStorableClass: Class[T]
                                    ): DataFrame = {

    getsAsDF(table, rowkeys.collect(), hbaseStorableClass)
  }
//
//  def getsAsRDD[T <: HBaseStorable] (table: String,
//                                         rowkeys: RDD[_ <: Object],
//                                         hbaseStorableClass: Class[T]
//                                        ): RDD[T] = {
//
//    val coll = rowkeys.map{x=>
//      if (x.isInstanceOf[Array[Byte]]) {
//        new Get(x.asInstanceOf[Array[Byte]])
//      }
//      else if (x.isInstanceOf[String]) {
//        new Get(Bytes.toBytes(x.asInstanceOf[String]))
//      } else if (x.isInstanceOf[Int]) {
//        new Get(Bytes.toBytes(x.asInstanceOf[Int]))
//      } else if (x.isInstanceOf[Long]) {
//        new Get(Bytes.toBytes(x.asInstanceOf[Long]))
//      } else {
//        throw new HBaseClientException(s"Rowkey type '${x.getClass.getName}' is not supported when batch get operating")
//      }
//    }.mapPartitions[HBaseStorable]{x=>
//
//      val _directHBaseConf = HBaseConfiguration.create
//      val _directHBaseConnection = ConnectionFactory.createConnection(_directHBaseConf)
//      val _t = _directHBaseConnection.getTable(TableName.valueOf(table))
//      val res = _t.get(x.toList)
//        .filter{!_.isEmpty}
//        .map { z =>
//          HBaseClient.assemble(z, hbaseStorableClass)
//        }
//      _t.close()
//      res.toIterator
//    }.asInstanceOf[RDD[T]].collect()
//
//  }

  private def createWriteConf (table: String): Configuration = {
    val conf = HBaseConfiguration.create

    config
      .getConfig("hbase.set")
      .entrySet()
      .foreach { x =>
        conf.set(x.getKey, x.getValue.unwrapped().toString)
      }
    //conf.set("hbase.zookeeper.quorum", config.getConfig("hbase.set").getString("hbase.zookeeper.quorum"))
    //conf.set("hbase.zookeeper.property.clientPort", config.getConfig("hbase.set").getString("hbase.zookeeper.property.clientPort"))
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.getConfiguration
  }

  def scanAsDF (table: String,
                startRow: Array[Byte],
                stopRow: Array[Byte],
                clazz: Class[_ <: HBaseStorable]
//              ,structType: org.apache.spark.sql.types.StructType
               ): DataFrame = {

    val rdd = scan0(table, startRow, stopRow, clazz)

    if (rdd.isEmpty) {
      val e = hiveContext.sparkContext.emptyRDD[org.apache.spark.sql.Row]
      return hiveContext.createDataFrame(e, clazz.newInstance().structType)
      //return hiveContext.createDataFrame(e, structType)
    }

    hiveContext.createDataFrame(rdd, clazz)
  }

  private def scan0 (table: String,
                         startRow: Array[Byte],
                         stopRow: Array[Byte],
                         clazz: Class[_ <: HBaseStorable]
                        ): RDD[_ <: HBaseStorable] = {


    val transactionalLegacyDataBackupCompletedTableSign = "_backup_completed_"
    val transactionalLegacyDataBackupProgressingTableSign = "_backup_progressing_"

    val backupCompletedTableNameWithinClassNamePattern = "_class_(.+)".r

    val transactionalTmpTableSign = "_trans_"

    val scan = new Scan()
    if (startRow != null) scan.setStartRow(startRow)
    if (stopRow != null) scan.setStopRow(stopRow)

    val conf = createReadConf(table, scan)

    var sc = this.sc
    val rdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //HBaseStorable
    //    rdd.map { case (b, result) =>
    //      assemble(result, clazz)
    //    }

    rdd
      .map { x =>
        HBaseClient.assemble(x._2, clazz)
      }
      .filter { x => x != null }
  }


  def putsNonTransaction (table: String, hbaseStorables: RDD[_ <: HBaseStorable]): Unit = {

    val conf = createWriteConf(table)
    hbaseStorables
      .map{ x =>
        val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)
        x.toColumns.map { x =>
          put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
        }
        (new ImmutableBytesWritable, put)
      }
//      .repartition(3)
      .saveAsNewAPIHadoopDataset(conf)
  }

  def putsNonTransaction (table: String, hbaseStorables: Array[_ <: HBaseStorable]): Unit = {

    LOG.warn("hbase putsNonTransaction start, count", hbaseStorables.length)
    val conf = createWriteConf(table)
    sc.parallelize(hbaseStorables.map { x =>

      val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)//.setWriteToWAL(false)
      x.toColumns.map { x =>
        put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
      }
      (new ImmutableBytesWritable, put)

    }).repartition(3).saveAsNewAPIHadoopDataset(conf)

    LOG.warn("hbase putsNonTransaction done")
  }

  def deleteTables (tablePattern: String): Unit = {
    directHBaseAdmin.disableTables(Pattern.compile(tablePattern))
    directHBaseAdmin.deleteTables(Pattern.compile(tablePattern))
  }

  def deleteTable (tableName: TableName): Unit = {
    if (directHBaseAdmin.isTableAvailable(tableName)) {
      directHBaseAdmin.disableTable(tableName)
      directHBaseAdmin.deleteTable(tableName)
    }
  }

  def createTableIfNotExists (table: String, like: String): Boolean = {

    if (directHBaseAdmin.isTableAvailable(TableName.valueOf(table))) return false

    val likeT = directHBaseAdmin.getTableDescriptor(TableName.valueOf(like))

    val desc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(table))

    likeT.getFamilies.foreach { x =>
      desc.addFamily(x)
    }
    directHBaseAdmin.createTable(desc)
    return true
  }

  def deleteTable(table: String ) :Unit = {
    deleteTable(TableName.valueOf(table))
  }

  def renameTable (table: String, newTableName: String): Unit = {
    val s = table + "_snapshot"
    val t = TableName.valueOf(table)
    directHBaseAdmin.disableTable(t)
    directHBaseAdmin.snapshot(s, t)
    directHBaseAdmin.cloneSnapshot(s, TableName.valueOf(newTableName))
    directHBaseAdmin.deleteSnapshot(s)
    directHBaseAdmin.deleteTable(t)
  }

  def puts (transactionParentId: String, table: String, hbaseStorables: Array[_ <: HBaseStorable]): HBaseTransactionCookie = {
    puts(transactionParentId, table, hbaseStorables, null)
  }

  def puts (transactionParentId: String, table: String, hbaseStorables: Array[_ <: HBaseStorable], hbaseStorablesComponentType:Class[_ <: HBaseStorable]): HBaseTransactionCookie = {

    val tid = transactionManager.generateTransactionId(transactionParentId)

    try {

      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      var hsc = if(hbaseStorablesComponentType != null) hbaseStorablesComponentType else hbaseStorables.getClass.getComponentType
      val ct = table + "_class_" + hsc.getName.replaceAll("\\.", "_") + transactionalLegacyDataBackupCompletedTableSign + tid

      LOG.warn("hbase puts start, count", hbaseStorables.length)
      moduleTracer.trace("    hbase puts start")
      createTableIfNotExists(tt, table)
      moduleTracer.trace("    hbase create table")

      val conf = createWriteConf(tt)

      sc.parallelize(hbaseStorables.map { x =>

        val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)
        x.toColumns.map { x =>
          put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
        }
        (new ImmutableBytesWritable, put)

      }).saveAsNewAPIHadoopDataset(conf)

      LOG.warn("hbase puts done")
      moduleTracer.trace("    hbase save")

      new HBaseTransactionCookie(
        transactionParentId,
        tid,
        tt,
        table,
        hsc.asInstanceOf[Class[HBaseStorable]],
        pt,
        ct,
        hbaseStorables.length
      )
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HBaseClientException(s"HBase Puts Fail, module: $moduleName, transactionId: " + tid, e)
    }
  }

  override def commit (cookie: TransactionCookie): Unit = {

    if(cookie == null) return

    LOG.warn("hbase commit start")
    moduleTracer.trace("    hbase commit start")

    val c = cookie.asInstanceOf[HBaseTransactionCookie]

    //Back up legacy data for possible rollback
    try {
      val s = scan0(
        c.transactionalTmpTable,
        null,
        null,
        c.hbaseStorableImplClass
      ).collect()

      moduleTracer.trace("    hbase scan trans-pluggable")

      val r = gets(
        c.targetTable,
        s.map { x =>
          x.toHBaseRowkey
        },
        c.hbaseStorableImplClass
      )

      createTableIfNotExists(c.transactionalProgressingBackupTable, c.targetTable)
      moduleTracer.trace("    hbase create table")
      LOG.warn("hbase create table done")

      putsNonTransaction(c.transactionalProgressingBackupTable, r.toArray[HBaseStorable](new Array[HBaseStorable](0)))
      moduleTracer.trace("    hbase save backup")
      LOG.warn("hbase save backup done, count", r.length)

      renameTable(c.transactionalProgressingBackupTable, c.transactionalCompletedBackupTable)
      moduleTracer.trace("    hbase rename")
      LOG.warn("hbase rename done, count")

      //Commit critical code
      putsNonTransaction(c.targetTable, s)
      moduleTracer.trace("    hbase save target")

      LOG.warn("hbase commit done")
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HBaseClientException(s"HBase Transaction Commit Fail, module: $moduleName, cookie: " + cookie, e)
    }

  }

  def createReadConf (table: String, scan: Scan): Configuration = {
    val p = ProtobufUtil.toScan(scan);
    val scanStr = Base64.encodeBytes(p.toByteArray());

    val conf = HBaseConfiguration.create()
    config
      .getConfig("hbase.set")
      .entrySet()
      .foreach { x =>
        conf.set(x.getKey, x.getValue.unwrapped().toString)
      }
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.set("hbase.zookeeper.property.clientPort", config.getConfig("hbase.set").getString("hbase.zookeeper.property.clientPort"))
    // conf.set("hbase.zookeeper.quorum", config.getConfig("hbase.set").getString("hbase.zookeeper.quorum"))
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set(TableInputFormat.SCAN, scanStr)
    conf
  }

  private def delete0 (table: String, deletes: Array[_ <: HBaseStorable]): Unit = {

    LOG.warn(s"HBaseClient delete0 start, count", deletes.length)
    val t = directHBaseConnection.getTable(TableName.valueOf(table))

    val ds = new util.ArrayList[Delete](deletes.length)

    deletes.foreach { x =>
      ds.add(new Delete(x.toHBaseRowkey))
    }

    t.delete(ds)
    t.close()
    LOG.warn(s"HBaseClient delete0 done")
  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    try {
      val cleanable = new Cleanable()
      if (cookies.isEmpty) {

        LOG.warn(s"HBaseClient rollback started(cookies is empty)")
        //Revert to the legacy data!!
        directHBaseAdmin
          .listTables(Pattern.compile(s".*$transactionalLegacyDataBackupCompletedTableSign.*"))
          .sortBy{x=>(
              x.getNameAsString.split(transactionalLegacyDataBackupCompletedTableSign)(1)
                .split(TransactionManager.parentTransactionIdSeparator)(0),
              Integer.parseInt(x.getNameAsString
                .split(transactionalLegacyDataBackupCompletedTableSign)(1)
                .split(TransactionManager.parentTransactionIdSeparator)(1))
            )}
          .reverse
          .foreach { x =>

            val t = x.getNameAsString
            val table = t.replaceFirst(backupCompletedTableNameForTransactionalTargetTableNameReplacePattern, "")

            val parentTid = t
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0)

            if (!transactionManager.isCommited(parentTid)) {
              val cn = backupCompletedTableNameWithinClassNamePattern
                .findAllIn(t)
                .matchData
                .map { x =>
                  x.group(1)
                }
                .next()
                .replaceAll("_", ".")

              val c = Class.forName(cn).asInstanceOf[Class[HBaseStorable]]

              val b = scan0(
                t,
                null,
                null,
                c
              ).collect()

              //针对新增的数据，需删除掉
              val tt = t.replaceFirst(backupCompletedTableNameForTransactionalTmpTableNameReplacePattern, transactionalTmpTableSign)
              LOG.warn(s"HBaseClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${tt}")
              val r = scan0(
                tt,
                null,
                null,
                c
              )
              //Delete
              delete0(table, r.collect())

              //针对被更新的数据，回滚到以前的值
              if (!b.isEmpty) {
                LOG.warn(s"HBaseClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${t}")
                //Overwrite by backup data
                putsNonTransaction(table, b)
              }

            }
          }

        deleteTables(s".*$transactionalLegacyDataBackupCompletedTableSign.*")
        //Delete it if exists
        deleteTables(s".*$transactionalLegacyDataBackupProgressingTableSign.*")
        deleteTables(s".*$transactionalTmpTableSign.*")
        return cleanable
      }

      LOG.warn(s"HBaseClient rollback started(cookies not empty)")
      val cs = cookies.asInstanceOf[Array[HBaseTransactionCookie]]
      cs.foreach { x =>

        val parentTid = x.id.split(TransactionManager.parentTransactionIdSeparator)(0)

        if (!transactionManager.isCommited(parentTid)) {
          //Revert to the legacy data!!
          val b = scan0(
            x.transactionalCompletedBackupTable,
            null,
            null,
            x.hbaseStorableImplClass
          ).collect()

          if (b.isEmpty) {
            //Delete
            val tt = x.transactionalTmpTable //t.replaceFirst(backupCompletedTableNameForTransactionalTmpTableNameReplacePattern, transactionalTmpTableSign)
            LOG.warn(s"HBaseClient rollback", s"Revert to the legacy data, Delete by transactionalTmpTable: ${tt}")
            val r = scan0(
              tt,
              null,
              null,
              x.hbaseStorableImplClass
            )
            delete0(x.targetTable, r.collect())
          } else {
            //Overwrite
            LOG.warn(s"HBaseClient rollback", s"Revert to the legacy data, Overwrite by backup table: ${b}")
            putsNonTransaction(x.targetTable, b)
          }

        }
      }
      cs.foreach { x =>
        deleteTable(TableName.valueOf(x.transactionalCompletedBackupTable))
        //Delete it if exists
        deleteTable(TableName.valueOf(x.transactionalProgressingBackupTable))
        deleteTable(TableName.valueOf(x.transactionalTmpTable))
      }

      LOG.warn(s"HBaseClient rollback completed")
      cleanable
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HBaseClientException(s"HBase Transaction Rollback Fail, module: $moduleName, cookies: " + cookies, e)
    }
  }

  override def clean (cookies: TransactionCookie*): Unit = {

    try {
      cookies.foreach{x=>
        if(x.isInstanceOf[HBaseTransactionCookie]) {
          var c = x.asInstanceOf[HBaseTransactionCookie]
          deleteTable(TableName.valueOf(c.transactionalCompletedBackupTable))
          //Delete it if exists
          deleteTable(TableName.valueOf(c.transactionalProgressingBackupTable))

          deleteTable(TableName.valueOf(c.transactionalTmpTable))
        }
      }
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HBaseClientException(s"HBase Transaction Clean Fail, module: $moduleName, cookie: " + OM.toJOSN(cookies), e)
    }
  }

}

object HBaseClient {
  private def assemble[T <: HBaseStorable] (result: Result, clazz: Class[T]): T = {
    if (result == null || result.isEmpty) {
      return null.asInstanceOf[T]
    }
    val h = clazz.newInstance()
    //h.setRowkey(result.getRow)

    var map = Map[(String, String), Array[Byte]]()
    result.getNoVersionMap.foreach { x =>
      x._2.foreach { y =>
        map += ((Bytes.toString(x._1) -> Bytes.toString(y._1)) -> y._2)
      }
    }
    h.assembleFields(result.getRow, map)
    h
  }
}