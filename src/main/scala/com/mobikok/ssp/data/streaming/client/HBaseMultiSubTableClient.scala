package com.mobikok.ssp.data.streaming.client

import java.util
import java.util.Date
import java.util.concurrent.{CopyOnWriteArrayList, CopyOnWriteArraySet, ExecutorService}
import java.util.regex.Pattern

import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.exception.HBaseClientException
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, FilterList, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
  * 暂时只支持单客户端，多可客户端有并发问题
  * Created by Administrator on 2017/6/8.
  */
class HBaseMultiSubTableClient (moduleName: String, sc: SparkContext, config: Config, transactionManager: MixTransactionManager, moduleTracer: ModuleTracer) extends HBaseClient(
  moduleName , sc, config, transactionManager, moduleTracer
) with Transactional {
  //  private[this] val LOG = Logger.getLogger(getClass().getName())
  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  @volatile var hiveContext: HiveContext = null;

  @volatile var directHBaseConf: Configuration = null
  @volatile var directHBaseConnection: Connection = null
  @volatile var directHBaseAdmin: Admin = null

  var threadPool: ExecutorService = null

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"

  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"

  private val backupCompletedTableNameWithinClassNamePattern = ("_class_(.+)" + transactionalLegacyDataBackupCompletedTableSign).r

  private val backupCompletedTableNameForTransactionalTmpTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign
  private val backupCompletedTableNameForTransactionalTargetTableNameReplacePattern = "_class_.+" + transactionalLegacyDataBackupCompletedTableSign + ".*"

  @volatile var inited = false
  override def init (): Unit = {
    new Thread(new Runnable {
      override def run (): Unit = {
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
        threadPool = ExecutorServiceUtil.createdExecutorService(10000)
        inited = true
      }
    }).start()

  }

  def waitInit(): Unit ={
    while (!inited) {
      Thread.sleep(1000L)
    }
  }

  override def gets[T <: HBaseStorable] (table: String,
                                rowkeys: Array[_ <: Object],
                                hbaseStorableClass: Class[T]
                               ): util.ArrayList[T] = {
    waitInit()

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

  def getsMultiSubTable[T <: HBaseStorable] (table: String,
                                             rowkeys: Array[_ <: Object],
                                             hbaseStorableClass: Class[T]
                                            ): util.ArrayList[T] = {
    waitInit()

    val res: util.ArrayList[T] = new util.ArrayList[T]()

    val reg = table + ".*"
    val subTs = directHBaseAdmin.listTableNames(reg)

//    subTs.par.foreach{x=>
//      res.addAll(gets0(x.getNameAsString, rowkeys, hbaseStorableClass))
//    }

    val tsArray = subTs.par
    tsArray.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(10000))
    tsArray.foreach{ x =>
      res.addAll(gets0(x.getNameAsString, rowkeys, hbaseStorableClass))
    }

    res
  }

  def getsMultiSubTableAsDF[T <: HBaseStorable] (table: String,
                                             rowkeys: Array[_ <: Object],
                                             hbaseStorableClass: Class[T]
                                            ): DataFrame = {
    waitInit()
    LOG.warn("hbase table getsMultiSubTableAsDF start", "result count:", rowkeys.length,  "table", table, "clazz", hbaseStorableClass.getName)

    val r: util.Set[T] = new CopyOnWriteArraySet[T]()

    val reg = table + ".*"
    val subTs = directHBaseAdmin.listTableNames(reg)

//    subTs.par.foreach{x=>
//      r.addAll(gets0(x.getNameAsString, rowkeys, hbaseStorableClass))
//    }
    subTs.par.foreach{ x =>
      r.addAll(gets0(x.getNameAsString, rowkeys, hbaseStorableClass))
    }

    if (r.isEmpty) {
      val e = hiveContext.sparkContext.emptyRDD[org.apache.spark.sql.Row]
      return hiveContext.createDataFrame(e, hbaseStorableClass.newInstance().structType)
    }

    LOG.warn("hbase table getsMultiSubTableAsDF", "rowkey count: " , rowkeys.length, "result count:", r.size(), "take(1)", r.take(1))

    val res = hiveContext.createDataFrame(
      r.map { x =>
        x.toSparkRow()
      }.toList,
      hbaseStorableClass.newInstance().structType
    )

    moduleTracer.trace("    hbase getsMultiSubTableAsDF done")
    LOG.warn("hbase table getsMultiSubTableAsDF done")

    res
  }

  private def gets0[T <: HBaseStorable] (table: String,
                                         rowkeys: Array[_ <: Object],
                                         hbaseStorableClass: Class[T]
                                        ): util.ArrayList[T] = {

    waitInit()
    //moduleTracer.trace("    hbase gets0 start")
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

      if (!x.isEmpty) r.add(HBaseMultiSubTableClient.assemble(x, hbaseStorableClass))
    }
    t.close()


    //moduleTracer.trace("    hbase gets0 done")
    LOG.warn("hbase gets0 done, result count", r.size)

    r
  }

  private def scan0[T <: HBaseStorable] (tableName: String,
                                         rowkeys: Array[_ <: Object],
                                         hbaseStorableClass: Class[T]): util.ArrayList[T] = {
    waitInit()
    LOG.warn("hbase scan0 start", s"table: $tableName\nrowkeys count: ${rowkeys.length}")

    val table = directHBaseConnection.getTable(TableName.valueOf(tableName))
    val filterList = new FilterList()

    rowkeys.foreach{ key =>
      if (key.isInstanceOf[Array[Byte]]) {
        filterList.addFilter(new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(new String(key.asInstanceOf[Array[Byte]])))))
      } else if (key.isInstanceOf[String]) {
        filterList.addFilter(new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(key.asInstanceOf[String]))))
      } else if (key.isInstanceOf[Int]) {
        filterList.addFilter(new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(key.asInstanceOf[Int]))))
      } else if (key.isInstanceOf[Long]) {
        filterList.addFilter(new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(key.asInstanceOf[Long]))))
      } else {
        throw new HBaseClientException(s"Rowkey type '${key.getClass.getName}' is not supported when batch get operating")
      }
    }
    val scan = new Scan()
    scan.setFilter(filterList)
    val resultList = new util.ArrayList[T]()

    table.getScanner(scan).foreach{ result =>
      if (!result.isEmpty) {
        resultList.add(HBaseMultiSubTableClient.assemble(result, hbaseStorableClass))
      }
    }

    resultList
  }

  override def getsAsDF[T <: HBaseStorable] (table: String,
                                    ids: Array[_ <: Object],
                                    hbaseStorableClass: Class[T]
                                   ): DataFrame = {

    waitInit()

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

  override def getsAsDF[T <: HBaseStorable] (table: String,
                                     rowkeys: RDD[_ <: Object],
                                     hbaseStorableClass: Class[T]
                                    ): DataFrame = {
    waitInit()
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
    waitInit()

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

  override def scanAsDF (table: String,
                startRow: Array[Byte],
                stopRow: Array[Byte],
                clazz: Class[_ <: HBaseStorable]
//              ,structType: org.apache.spark.sql.types.StructType
               ): DataFrame = {

    waitInit()

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

    waitInit()

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
        HBaseMultiSubTableClient.assemble(x._2, clazz)
      }
      .filter { x => x != null }
  }

  private var SUB_TABLE_DAY_FORMAT = CSTTime.formatter("yyyyMMdd")
  private val maxSubTableNum = 7;

  def createSubTableIfNotExists (table: String, date: Date): String ={
    waitInit()

    val subTable = subTableName(table, date)
    val created = createTableIfNotExists(subTable, table)

    // 如果超过最大保留子表数，则删除早期的子表
    if(true/*created*/) {
      // If table name: SSP_SEND_DWI_PHOENIX, Then the sub table name eg: SSP_SEND_DWI_PHOENIX_20180126
      val ss = directHBaseAdmin.listTableNames(table + "_.{"+(SUB_TABLE_DAY_FORMAT.toPattern + "").length+"}$")
      if(ss.length > maxSubTableNum) {
        val c = ss.length - maxSubTableNum
        val dels = ss.sortBy(_.getNameAsString).zipWithIndex.filter{case (t, i) => i < c }
        LOG.warn("Deleting expired tables", dels.map{case(t, i) => t.getNameAsString})

        dels.foreach{case(t, i)=>
          directHBaseAdmin.disableTable(t)
          directHBaseAdmin.deleteTable(t)
          LOG.warn("HBase expired table delete done", t)
        }
      }
    }

    subTable
  }
  private def subTableName (table: String, date: Date): String ={
    var day = SUB_TABLE_DAY_FORMAT.format(date) // "20180105";
    s"${table}_${day}"
  }

  def createCurrSubTableIfNotExists (table: String): String ={
    waitInit()

    createSubTableIfNotExists(table, new Date())
  }

  def putsNonTransactionMultiSubTable (table: String, hbaseStorables: RDD[_ <: HBaseStorable]): Unit = {

    waitInit()

    val subTable = createCurrSubTableIfNotExists(table)
    LOG.warn("hbase putsNonTransaction start", "table", table, "sub_table", subTable/*, "count", hbaseStorables.count()*//*, "toColumns take(2)", hbaseStorables.take(2).map{x=>x.toColumns}*/)

    RunAgainIfError.run({
      val conf = createWriteConf(subTable)
      hbaseStorables
        .map{ x =>
          val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)
          // filter用于区别空字符串与null的表示，空字符串时对应的列名依然存储，null值直接忽略掉所有
          x.toColumns.filter(_._2 != null).map { x =>
            put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
          }
          (new ImmutableBytesWritable, put)
        }
  //      .repartition(3)
        .saveAsNewAPIHadoopDataset(conf)
    })

    sinceLastTimeFlushBatchPutsCount += 1

    new Thread(new Runnable {
      override def run(): Unit = {
//        flush(subTable)
      }
    }).start()

    LOG.warn("hbase putsNonTransaction done" )
  }

  @volatile private var sinceLastTimeFlushBatchPutsCount = 0
  @volatile private var isFlushing = false

//  private var executorService = ExecutorServiceUtil.createdExecutorService(1)

  @volatile var flushLocks = new util.HashMap[String, Object]()
  def flush(table: String): Unit ={
    var lock: Object = null
      this.synchronized{
      lock = flushLocks.get(table)
      if(lock == null) {
        lock = new Object
        flushLocks.put(table, lock)
      }
    }
    var needFlush = false
    lock.synchronized{
      if(!isFlushing) {
        var interval = 0
        val inte = MC.pullUpdatable("hbase_flush_cer", Array("hbase_flush_interval"))
        if(StringUtil.notEmpty(inte)) {
          interval = inte.toInt
        }
        if(sinceLastTimeFlushBatchPutsCount >= interval) {
          needFlush = true
        }
      }
    }
    if(needFlush) {
      LOG.warn("HBase flush start", "table", table, "putsTimesCount", sinceLastTimeFlushBatchPutsCount)
      sinceLastTimeFlushBatchPutsCount = 0
      isFlushing = true
      directHBaseAdmin.flush(TableName.valueOf(table))
      LOG.warn("HBase flush done", "subTable", table)
    }
  }

  override def putsNonTransaction (table: String, hbaseStorables: RDD[_ <: HBaseStorable]): Unit = {
    waitInit()

    LOG.warn("hbase putsNonTransaction start" /*,"count", hbaseStorables.count(),*/ /*"toColumns take(2)", hbaseStorables.take(2).map{x=>x.toColumns}*/)
    val conf = createWriteConf(table)
    hbaseStorables
      .map{ x =>
        val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)
        // filter用于区别空字符串与null的表示，空字符串时对应的列名依然存储，null值直接忽略掉所有
        x.toColumns.filter(_._2 != null).map { x =>
          put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
        }
        (new ImmutableBytesWritable, put)
      }
//      .repartition(3)
      .saveAsNewAPIHadoopDataset(conf)

    LOG.warn("hbase putsNonTransaction done")
  }

  override def putsNonTransaction (table: String, hbaseStorables: Array[_ <: HBaseStorable]): Unit = {
    waitInit()

    LOG.warn("hbase putsNonTransaction start", "count", hbaseStorables.length/*, "toColumns take(2)", hbaseStorables.take(2).map{x=>x.toColumns}*/)
    val conf = createWriteConf(table)
    sc.parallelize(hbaseStorables.map { x =>

      val put = new Put(/*Bytes.toBytes(Bytes.toString(x.getHBaseRowkey))*/x.toHBaseRowkey).setDurability(Durability.SKIP_WAL)//.setWriteToWAL(false)
      // filter用于区别空字符串与null的表示，空字符串时对应的列名依然存储，null值直接忽略掉所有
      x.toColumns.filter(_._2 != null).map { x =>
        put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
      }
      (new ImmutableBytesWritable, put)

    }).repartition(3).saveAsNewAPIHadoopDataset(conf)

    LOG.warn("hbase putsNonTransaction done")
  }

  override def deleteTables (tablePattern: String): Unit = {
    waitInit()

    directHBaseAdmin.disableTables(Pattern.compile(tablePattern))
    directHBaseAdmin.deleteTables(Pattern.compile(tablePattern))
  }

  override def deleteTable (tableName: TableName): Unit = {
    waitInit()

    if (directHBaseAdmin.isTableAvailable(tableName)) {
      directHBaseAdmin.disableTable(tableName)
      directHBaseAdmin.deleteTable(tableName)
    }
  }

  import org.apache.hadoop.hbase.util.Bytes

  private def getSplitKeys = {

//    val keys = Array[String](
//      "08", "18", "28", "38", "48", "58", "68", "78",
//      "88", "98", "a8", "b8", "c8", "d8", "e8", "f8")


    //    val keys = Array[String](
//      "08", "11", "18", "21", "28", "31", "38", "41", "48", "51", "58" ,"61", "68" ,"71", "78",
//      "81", "88", "91", "98", "a1", "a8", "b1", "b8", "c1", "c8", "d1", "d8", "e1", "e8" ,"f1", "f8")

    //0 1 2 3    4 (5) 6 7    8 9 (a) b   c d e f
//    val keys = Array[String](
//      "05", "0a",
//      "10", "15", "1a",   "20", "25", "2a",   "30", "35" ,"3a",
//      "40", "45", "4a",   "50", "55", "5a",   "60", "65" ,"6a",
//      "70", "75", "7a",   "80", "85", "8a",   "90", "95" ,"9a",
//      "a0", "a5", "aa",   "b0", "b5", "ba",   "c0", "c5" ,"ca",
//      "d0", "d5", "da",   "e0", "e5", "ea",   "f0", "f5" ,"fa"
//     )

    val keys = Array[String](
      "05",
      "10", "1a", "25", "30", "3a",
      "45", "50", "5a", "65" ,
      "70", "7a", "85", "90", "9a",
      "a5", "b0", "ba", "c5" ,
      "d0", "da", "e5", "f0","fa"
    )

    val splitKeys = new Array[Array[Byte]](keys.length)
    val rows = new util.TreeSet(Bytes.BYTES_COMPARATOR)
    //升序排序
    var i = 0
    while ( {
      i < keys.length
    }) {
      rows.add(Bytes.toBytes(keys(i)))

      {
        i += 1; i - 1
      }
    }
    val rowKeyIter = rows.iterator
    var j = 0
    while ( {
      rowKeyIter.hasNext
    }) {
      val tempRow = rowKeyIter.next
      rowKeyIter.remove
      splitKeys(j) = tempRow
      j += 1
    }
    splitKeys
  }

  override def createTableIfNotExists (table: String, like: String): Boolean = {
    waitInit()

    if (directHBaseAdmin.isTableAvailable(TableName.valueOf(table))) return false

    val likeT = directHBaseAdmin.getTableDescriptor(TableName.valueOf(like))

    val desc = new HTableDescriptor(TableName.valueOf(table))
//    desc.setRegionReplication(5)
    likeT.getFamilies.foreach { x =>
      x.setBloomFilterType(BloomType.ROW)
      x.setBlockCacheEnabled(true)
      x.setCompressionType(Compression.Algorithm.SNAPPY)
      desc.addFamily(x)
    }
    directHBaseAdmin.createTable(desc, getSplitKeys)

    return true
  }

  override def renameTable (table: String, newTableName: String): Unit = {
    waitInit()

    val s = table + "_snapshot"
    val t = TableName.valueOf(table)
    directHBaseAdmin.disableTable(t)
    directHBaseAdmin.snapshot(s, t)
    directHBaseAdmin.cloneSnapshot(s, TableName.valueOf(newTableName))
    directHBaseAdmin.deleteSnapshot(s)
    directHBaseAdmin.deleteTable(t)
  }

  override def puts (transactionParentId: String, table: String, hbaseStorables: Array[_ <: HBaseStorable]): HBaseTransactionCookie = {
    puts(transactionParentId, table, hbaseStorables, null)
  }

  override def puts (transactionParentId: String, table: String, hbaseStorables: Array[_ <: HBaseStorable], hbaseStorablesComponentType:Class[_ <: HBaseStorable]): HBaseTransactionCookie = {
    waitInit()
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
    waitInit()
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

      moduleTracer.trace("    hbase scan trans-tmp")

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

  override def createReadConf (table: String, scan: Scan): Configuration = {
    waitInit()
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
    waitInit()

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
    waitInit()

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
    waitInit()
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



object HBaseMultiSubTableClient {
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
