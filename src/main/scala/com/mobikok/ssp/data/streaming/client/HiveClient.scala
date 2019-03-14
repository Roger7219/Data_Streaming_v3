package com.mobikok.ssp.data.streaming.client

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.ExecutorService
import java.util.{Date, List, UUID}

import com.facebook.fb303.FacebookService
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.{HBaseClientException, HiveClientException}
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.module.support.{HiveContextGenerater, OptimizedTransactionalStrategy, TransactionalStrategy}
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HTableDescriptor, TableName}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.Ordering

/**
  * Created by Administrator on 2017/6/8.
  */
class HiveClient(moduleName:String, config: Config, ssc: StreamingContext, messageClient: MessageClient, transactionManager: TransactionManager, moduleTracer: ModuleTracer) extends Transactional {

  //private[this] val LOG = Logger.getLogger(getClass().getName());

  private var hiveJDBCClient: HiveJDBCClient = null
  var hiveContext: HiveContext = null
  var compactionHiveContext:HiveContext = null
  var hdfsUtil: HdfsUtil = null

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"
  val shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  //  private val transactionalPartitionField = "tid"
  val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)

  override def init(): Unit = {
    LOG.warn(s"HiveClient init started")
    hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
    LOG.warn("show tables: ", hiveContext.sql("show tables").collect())

    compactionHiveContext = HiveContextGenerater.generate(ssc.sparkContext)

    hiveJDBCClient = new HiveJDBCClient(config.getString("hive.jdbc.url"), null, null)

    hdfsUtil = new HdfsUtil()

    tryReMoveCompactResultTempFile()

    //    config
    //      .getStringList("hive.transactional.tables")
    //      .foreach { x =>
    //        hiveContext.sql(s"CREATE TABLE IF NOT EXISTS $x$transactionalTmpTableSign LIKE $x")
    //      }
    LOG.warn(s"HiveClient init completed")
  }

  def createTableIfNotExists(table: String, like: String): Unit = {
    sql(s"create table if not exists $table like $like")
  }

  def into(transactionParentId: String, table: String, df: DataFrame, ps: Array[Array[HivePartitionPart]]): HiveTransactionCookie = {

    val fs = hiveContext.read.table(table).schema.fieldNames
    var tid: String = null
    try {

      tid = transactionManager.generateTransactionId(transactionParentId)

      val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient into repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
//      val parts = sparkPartitionNum("HiveClient into repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
      moduleTracer.trace(s"    into repartition fs: $fileNumber, rs: ${df.rdd.getNumPartitions}, ps: ${ps.length}")

      if(!transactionManager.needTransactionalAction()) {

        df.selectExpr(fs:_*)
          .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Append)
          .insertInto(table)
        return new HiveNonTransactionCookie(transactionParentId, tid, table, ps)
      }

      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

      createTableIfNotExists(tt, table)

      df.selectExpr(fs:_*)
        .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
        .write
        .format("orc")
        .mode(SaveMode.Append)
        .insertInto(tt)

      moduleTracer.trace("    write transactional tmp table")

      new HiveRollbackableTransactionCookie(
        transactionParentId,
        tid,
        tt,
        table,
        SaveMode.Append,
        ps,
        pt,
        ct,
        df.take(1).isEmpty
      )
    } catch {
      case e: Exception =>
        LOG.warn(ExceptionUtil.getStackTraceMessage(e))
        throw new HiveClientException(s"Hive Insert Into Fail, module: $moduleName, transactionId: $tid, new DF schema: ${df.schema.treeString}" , e)
    }
  }

  def into(transactionParentId: String, table: String, df: DataFrame, tranPartition: String, tranPartitions: String*): HiveTransactionCookie = {
    val ts = tranPartitions :+ tranPartition
    val ps = df
      .dropDuplicates(ts)
      .rdd
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }.toArray
      }
      .collect()

    moduleTracer.trace("    get update partitions")
    into(transactionParentId, table, df, ps)
  }


  def overwriteUnionSum (transactionParentId: String,
                         table: String,
                         newDF: DataFrame,
                         aggExprsAlias: List[String],
                         unionAggExprsAndAlias: List[Column],
                         groupByFields: Array[String],
                         partitionField: String,
                         partitionFields: String*): HiveTransactionCookie = {

    val ts = Array(partitionField)  ++ partitionFields

    val ps = newDF
      .dropDuplicates(ts)
      .collect()
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }
    moduleTracer.trace("    get update partitions")

    overwriteUnionSum(transactionParentId, table, newDF, aggExprsAlias, unionAggExprsAndAlias, groupByFields, ps, partitionField, partitionFields:_*)
  }

  def overwriteUnionSum (transactionParentId: String,
                         table: String,
                         newDF: DataFrame,
                         aggExprsAlias: List[String],
                         unionAggExprsAndAlias: List[Column],
                         groupByFields: Array[String],
                         ps: Array[Array[HivePartitionPart]],
                         partitionField: String,
                         partitionFields: String*): HiveTransactionCookie = {

    var tid: String = null

    try {
      LOG.warn(s"HiveClient overwriteUnionSum start")

      tid = transactionManager.generateTransactionId(transactionParentId)
      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid


      val ts = Array(partitionField)  ++ partitionFields // l_time, b_date and b_time

      var w = partitionsWhereSQL(ps) // (l_time="2018-xx-xx xx:00:00" and b_date="2018-xx-xx" and b_time="2018-xx-xx xx:00:00")

      LOG.warn(s"HiveClient overwriteUnionSum overwrite partitions where", w)

      val original = hiveContext
        .read
        .table(table)
        .where(w)

      moduleTracer.trace("    read hive data for union")

      // Group by fields with partition fields
      val gs = (groupByFields :+ partitionField) ++ partitionFields

      var fs = original.schema.fieldNames
      var _newDF = newDF.select(fs.head, fs.tail:_*)
//      LOG.warn(s"HiveClient re-ordered field name via hive table _newDF take(2)", _newDF.take(2))

      // Sum
      val updated = original
        .union(_newDF /*newDF*/)
        .groupBy(gs.head, gs.tail:_*)
        .agg(
          unionAggExprsAndAlias.head,
          unionAggExprsAndAlias.tail:_*
        )
        .select(fs.head, fs.tail:_* /*groupByFields ++ aggExprsAlias ++ ts:_**/)


      val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient unionSum repartition", shufflePartitions, updated.rdd.getNumPartitions, ps.length)
//      val parts = sparkPartitionNum("HiveClient unionSum repartition", shufflePartitions, updated.rdd.getNumPartitions, ps.length)
      moduleTracer.trace(s"    union sum repartition fs: ${fileNumber}, rs: ${updated.rdd.getNumPartitions}, ps: ${ps.length}")

      if(transactionManager.needTransactionalAction()) {
        //支持事务，先写入临时表，commit()时在写入目标表
        createTableIfNotExists(tt, table)
        updated
//          .coalesce(1)
          .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(tt)

      }else {
        //非事务，直接写入目标表
        updated
//          .coalesce(1)
          .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(table)

        return new HiveNonTransactionCookie(transactionParentId, tid, table, ps)
      }


      val isE = newDF.take(1).isEmpty

      moduleTracer.trace("    union sum and write")

      new HiveRollbackableTransactionCookie(
        transactionParentId,
        tid,
        tt,
        table,
        SaveMode.Overwrite,
        ps,
        pt,
        ct,
        isE
      )
    } catch {
      case e: Exception =>
        LOG.warn(ExceptionUtil.getStackTraceMessage(e))
        throw new HiveClientException(s"Hive Insert Overwrite Fail, module: $moduleName, transactionId:  $tid", e)
    }
  }

  def overwrite(transactionParentId: String, table: String, df: DataFrame, partitionField: String, partitionFields: String*): HiveTransactionCookie = {

    val ts = partitionFields :+ partitionField

    val ps = df
      .dropDuplicates(ts)
      .rdd
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }.toArray
      }
      .collect()

    overwrite(transactionParentId, table, df, ps)
  }

  def overwrite(transactionParentId: String, table: String, df: DataFrame, ps: Array[Array[HivePartitionPart]]): HiveTransactionCookie = {

    var tid: String = null
    try {

      tid = transactionManager.generateTransactionId(transactionParentId)

      val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient overwrite repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
//      val parts = sparkPartitionNum("HiveClient overwrite repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
      moduleTracer.trace(s"    overwrite repartition fs: $fileNumber, rs: ${df.rdd.getNumPartitions}, ps: ${ps.length}")

      if(!transactionManager.needTransactionalAction()) {
        df.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Append)
          .insertInto(table)
        return new HiveNonTransactionCookie(transactionParentId, tid, table, ps)
      }

      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

      if(!df.take(1).isEmpty) {
        createTableIfNotExists(tt, table)
      }

      if(!df.take(1).isEmpty) {
        df.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(tt)
      }

      new HiveRollbackableTransactionCookie(
        transactionParentId,
        tid,
        tt,
        table,
        SaveMode.Overwrite,
        ps,
        pt,
        ct,
        df.take(1).isEmpty
      )
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HiveClientException(s"Hive Insert Overwrite Fail, module: $moduleName, transactionId: " + tid, e)
    }
  }

  def partitionsWhereSQL(ps : Array[Array[HivePartitionPart]]): String = {
    var w = ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString("(", " and ", ")")
    }.mkString(" or ")
    if ("".equals(w)) w = "1 = 1"
    w
  }

  def partitionsAlterSQL(ps : Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString(", ")
    }
  }

  private def lTimePartitionsAlterSQL(ps : Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.filter{z=>"l_time".equals(z.name)}
       .map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString(", ")
    }
  }


  override def commit(cookie: TransactionCookie): Unit = {
    if(cookie.isInstanceOf[HiveNonTransactionCookie]) {
      return
    }

    try {
      val c = cookie.asInstanceOf[HiveRollbackableTransactionCookie]

      LOG.warn(s"Hive before commit starting, cookie: ${OM.toJOSN(cookie)}")

      if(c.isEmptyData) {
        LOG.warn(s"Hive commit skiped, Because no data is inserted (into or overwrite) !!")
      }
      else {
        //Back up legacy data for possible rollback
        createTableIfNotExists(c.transactionalProgressingBackupTable, c.targetTable)

        var bw = c.partitions.map{x=>x.filter(y=>"l_time".equals(y.name))}

        //正常情况下只会有一个l_time值
        val flatBw = bw.flatMap{x=>x}.toSet
        if(flatBw.size != 1){
          throw new HiveClientException(s"l_time must be only one value for backup, But has ${flatBw.size} values: ${flatBw} !")
        }

        var w  = partitionsWhereSQL(bw)//partitionsWhereSQL(c.partitions)
  //      var w = c
  //        .partitions
  //        .map { x =>
  //          x.map { y =>
  //            y.name + "=\"" + y.value + "\""
  //            //y._1 + "=" + y._2
  //          }.mkString("(", " and ", ")")
  //        }
  //        .mkString(" or ")
  //
  //      if ("".equals(w)) w = "1 = 1"

        LOG.warn(s"Hive before commit backup table ${c.targetTable} where: ", bw)

        //For transactional rollback overwrite original empty partitions
        partitionsAlterSQL(c.partitions).foreach{x=>
          sql(s"alter table ${c.transactionalProgressingBackupTable} add partition($x)")
        }

        val backPs = sql(s"show partitions ${c.targetTable} partition(${flatBw.head.name}='${flatBw.head.value}')").count().toInt

        val df = hiveContext
          .read
          .table(c.targetTable)
          .where(w) //key code !!
//          .repartition(shufflePartitions) //?

        //hivePartitions不一定是df.rdd.getNumPartitions, 待优化
        val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient read for backup repartition", shufflePartitions, df.rdd.getNumPartitions, backPs)
//        val parts = sparkPartitionNum("HiveClient read for backup repartition", shufflePartitions, df.rdd.getNumPartitions, df.rdd.getNumPartitions)
        moduleTracer.trace(s"    read for backup repartition fs: $fileNumber, rs: ${df.rdd.getNumPartitions}, ps: ${backPs}")

        df.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(c.transactionalProgressingBackupTable)

        moduleTracer.trace("    insert into progressing backup table")
        sql(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")
        moduleTracer.trace("    rename to completed backup table")
  //      hiveContext.sql(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")

        //Commit critical code
        val df2 = hiveContext
          .read
          .table(c.transactionalTmpTable)

        val fileNumber2 = aHivePartitionRecommendedFileNumber("HiveClient read tmp table repartition", shufflePartitions, df2.rdd.getNumPartitions, c.partitions.length)
//        val parts2 = sparkPartitionNum("HiveClient read tmp table repartition", shufflePartitions, df2.rdd.getNumPartitions, c.partitions.length)
        moduleTracer.trace(s"    read tmp repartition fs: $fileNumber2, rs: ${df2.rdd.getNumPartitions}, ps: ${c.partitions.length}")

        df2
          .repartition(fileNumber2*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber2}) ) )"))
          .write
          .format("orc")
          .mode(c.saveMode)
          .insertInto(c.targetTable)

          moduleTracer.trace("    insert into target table")
      }

      tryAsyncCompaction(c)

    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HiveClientException(s"Hive Transaction Commit Fail, module: $moduleName,  cookie: " + cookie, e)
    }
  }

  val dataFormat = CSTTime.formatter("yyyy-MM-dd") //new SimpleDateFormat("yyyy-MM-dd")
  //table -> l_time -> needCompaction
  var yesterdayPartitionNeedCompactionMap = mutable.Map[String, mutable.Map[String, Boolean]]()

//  private var executorService = ExecutorServiceUtil.createdExecutorService(1)// must is 1

  private def tryAsyncCompaction(c: HiveRollbackableTransactionCookie): Unit = {

    val topic = s"compaction_${c.targetTable}"
    val cons = s"compactor_${c.targetTable}"
    //提交
    val ms = c.partitions.flatMap{x=>x}.filter{x=>"l_time".equals(x.name)}.distinct.map{x=>
      new MessagePushReq(topic, x.value, true, "")
    }
    messageClient.pushMessage(ms:_*)

    ThreadPool.execute{
      tryCompaction(c)
    }

    /*HiveClient.executorService.execute(new Runnable {
      override def run (): Unit = {
        //延迟半小时合并，尽量避开统计IO高峰期
        Thread.sleep(1000*60*5)
        tryCompaction(c)
      }
    })*/
  }

  private val compactedTmpTableSign = "__compact_result"
  private val compactingTmpTableSign = "__compact_doing"

  private def tryCompaction(c: HiveRollbackableTransactionCookie): Unit = {

    val topic = s"compaction_${c.targetTable}"
    val cons = s"compactor_${c.targetTable}"
    val compactedSuffix = ".compacted"

    //拉取
    var lts = messageClient
      .pullMessage(new MessagePullReq(cons, Array(topic) ))
      .getPageData

    if(lts.isEmpty) {
      return
    }

    lts = lts
      .sortBy{_.getKeyBody}(Ordering.String.reverse)
      .tail
      .sortBy{_.getOffset}

    LOG.warn(s"Hive compact table ${c.targetTable} starting, partitions", lts.toArray)

    val tt = s"${c.targetTable}${compactingTmpTableSign}"
    val compactedTt = s"${c.targetTable}$compactedTmpTableSign"

    lts.foreach{x=>

      sql(s"create table if not exists $tt like ${c.targetTable}")

      //合并，写入临时表
      compactionHiveContext
        .read
        .table(c.targetTable)
        .where(s""" l_time = "${x.getKeyBody}" """)
        .coalesce(1)
        .write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .insertInto(tt)

      // 原子操作
      sql(s"alter table $tt rename to $compactedTt")

      //将临时表中已经合并的数据移动到目标表
      sql(s""" show partitions ${compactedTt} partition(l_time = "${x.getKeyBody}" ) """)
        .collect()
        .foreach{x=>
          val srcPath = s"""/apps/hive/warehouse/${compactedTt}/${x.getAs[String]("partition")}"""

          val dstPath = s"""/apps/hive/warehouse/${c.targetTable}/${x.getAs[String]("partition")}"""


          //删除目标表中原文件(不含后缀的文件)
          val deleteFiles: List[String] = hdfsUtil.getFilePaths(dstPath)
          deleteFiles.map{x =>

            if(!x.endsWith(compactedSuffix)){
              val res = hdfsUtil.delete(x, false)

              if(res) LOG.warn("Hive compact: ", "deletefile ", x)
            }
          }

          //将临时表文件加后缀，并移动到目标表文件夹
          if(hdfsUtil.renameMove(compactedSuffix, srcPath, dstPath)){
            LOG.warn("Hive compact: renameMove", "suffix ", compactedSuffix, "srcPath ", srcPath, "dstPath", dstPath)
          }


//          //目标表文件去掉后缀
//          val replaceFiles: List[String] = deleteFiles//hdfsUtil.filter(dstPath, suffix, true)
//          replaceFiles.map{x =>
//
//            if(x.endsWith(compactedSuffix)){
//              val res = hdfsUtil.rename(x, x.split(compactedSuffix)(0))
//              if(res) LOG.warn("Hive compact: rename", "from ", x, "to ", x.split(compactedSuffix)(0))
//            }
//
//          }

          //1.删除需要合并的目标表l_time分区目录
         /* if(hdfsUtil.deleteDir(dstPath)){
            LOG.warn("Hive compact: ", "srcPath  ", srcPath, "deleteDir ", dstPath)
          }

          //2.将临时表中合并好的l_time分区数据，拷贝至相应目标表分区
          hdfsUtil.copyDir(srcPath, rootPath)

          LOG.warn("Hive compact: copy ", "from: ", srcPath, "to: ", dstPath)
          //3.删除临时表中已复制到目标表的l_time分区数据

          if(hdfsUtil.deleteDir(srcPath)){
            LOG.warn("Hive compact: ", "delete temp table ", srcPath)
          }*/

        }

      LOG.warn(s"Hive compact table ${c.targetTable} done, partition", x.getKeyBody)
//      moduleTracer.trace("    compaction table")

      //逐个提交
      messageClient.commitMessageConsumer(new MessageConsumerCommitReq(cons, topic, x.getOffset))

      //移动完成后删除临时表
      sql(s""" drop table if exists ${compactedTt} """)

    }

//    val cs = lts.map{x=>
//      new MessageConsumerCommitReq(cons, topic, x.getOffset)
//    }
//
//    messageClient.commitMessageConsumer(cs:_*)

  }

  def tryReMoveCompactResultTempFile() = {
    //判断是否存在临时表，若存在，则重新移动其中l_time分区数据至目标表中l_time分区

    HiveClient.lock.synchronized{

      val compactedSuffix = ".compacted"

      sql(s"show tables like '*$compactedTmpTableSign' ")
        .collect()
        .foreach{t =>
          val srcT = t.getAs[String]("tableName")

          val dstT = s"""${srcT.substring(0,srcT.indexOf(compactedTmpTableSign))}"""

          LOG.warn("Hive compact: ", "exsist tmp table ", srcT, "dstT ", dstT)

          //将临时表中所有分区的数据移动到目标表
          sql(s""" show partitions ${srcT}  """)
            .collect()
            .foreach{x=>

              val srcPath = s"""/apps/hive/warehouse/$srcT/${x.getAs[String]("partition")}"""

              val dstPath = s"""/apps/hive/warehouse/$dstT/${x.getAs[String]("partition")}"""

              //删除目标表中原文件(不含后缀的文件)
              val deleteFiles: List[String] = hdfsUtil.getFilePaths(dstPath)
              deleteFiles.map{x =>

                if(!x.endsWith(compactedSuffix)){
                  val res = hdfsUtil.delete(x, false)

                  if(res) LOG.warn("Hive compact: ", "deletefile ", x)
                }
              }

              //移动漏掉的文件到目标表
              if(hdfsUtil.move(srcPath, dstPath)){
                LOG.warn("Hive compact:move omit ", "from: ", srcPath, "to: ", dstPath)
              }

            }

          //删除遗留的临时表
          sql(s"drop table if exists ${srcT}")
          LOG.warn("Hive compact: first", "first drop table  ", srcT)

        }
    }

  }

  //pasrse sql like: repeated=N/l_time=2017-07-24 21%3A00%3A00/b_date=2017-07-24
  def parseShowPartition (partition: String): Array[HivePartitionPart] = {
    partition
      .split(s"/")
      .map{x=>
        val s = x.split("=")
        HivePartitionPart(URLDecoder.decode(s(0), "utf-8"), URLDecoder.decode(s(1), "utf-8"))
      }
  }
  def partitions(tableName:String): Array[Array[HivePartitionPart]] ={
    partitions(Array(tableName):_*)
  }
  def partitions(tableNames:String*): Array[Array[HivePartitionPart]] ={
    val ts = tableNames//tableName +: tableNames
    val ls = mutable.ListBuffer[Array[HivePartitionPart]]()
    ts.foreach{y=>
      val tt = y
      sql(s"show partitions $tt")
        .collect()
        .foreach{z=>
          ls += parseShowPartition(z.getAs[String]("partition"))
        }
    }
    ls.toArray[Array[HivePartitionPart]]
  }
//  def lTimePartitions(tableNames:String*): Array[Array[HivePartitionPart]] ={
//    val ts = tableNames//tableName +: tableNames
//    val ls = mutable.ListBuffer[Array[HivePartitionPart]]()
//    ts.foreach{y=>
//      val tt = y
//      sql(s"show partitions $tt")
//        .collect()
//        .foreach{z=>
//          ls += ( parseShowPartition(z.getAs[String]("partition")).filter{x=>"l_time".equals(x.name)} )
//        }
//    }
//    ls.toArray[Array[HivePartitionPart]]
//  }
  override def rollback (cookies: TransactionCookie*): Cleanable = {

    try {
      val cleanable = new Cleanable()
      if (cookies.isEmpty) {
        LOG.warn(s"HiveClient rollback started(cookies is empty)")
        //Revert to the legacy data!!
        val ct = sql(s"show tables like '*${transactionalLegacyDataBackupCompletedTableSign}*'")
//        hiveContext.sql(s"show tables like '*${transactionalLegacyDataBackupCompletedTableSign}*'")
          .collect

        LOG.warn("HiveClient rollback check backup completed table", ct.map{x=>x.getAs[String]("tableName")})

        ct.sortBy{x=>(
            x.getAs[String]("tableName")
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0),
            Integer.parseInt(x.getAs[String]("tableName")
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(1))
            )}
          .reverse
          .foreach { x =>

            val parentTid = x.getAs[String]("tableName")
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0)

            val tid = x.getAs[String]("tableName")
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)

            //Key code !!
            val needRollback = transactionManager.isActiveButNotAllCommited(parentTid)

            if (needRollback) {
              //val t = x.getAs[String]("database") + "." + x.getAs[String]("tableName")
              val t = x.getAs[String]("tableName")

              val bc = hiveContext
                .read
                .table(t)
                .count()

              val targetTable = t.split(transactionalLegacyDataBackupCompletedTableSign)(0)

              // +
              val ts =  Array(t)//sql(s"show tables like '*$targetTable$$targetTable$transactionalTmpTableSign$tid'").rdd.map(_.getAs[String]("tableName")).collect()
              val ps = partitions(ts:_*)

              lTimePartitionsAlterSQL(ps).distinct.foreach{y=>
                sql(s"alter table ${targetTable} drop if exists partition($y)")
              }

              // 应对新增字段问题，
              // 目标表新增了字段，backup表还是此前的表结构，
              // 所以需要识别新增字段，select backup表时添加上新增字段
              val targetFs = hiveContext.read.table(targetTable).schema.fields
              val targetStrFs = targetFs.map(_.toString())
              val backupFs = hiveContext.read.table(t).schema.fields
              val backupStrFs = backupFs.map(_.toString())
              val selects = ListBuffer[String]()
              targetStrFs.zipWithIndex.foreach{case(f, i)=>
                if(backupStrFs.contains(f)) {
                  selects.append(s"`${targetFs(i).name}`")
                }else {
                  selects.append(s"cast(null as ${targetFs(i).dataType.simpleString}) as `${targetFs(i).name}`")
                }
              }

              if(bc == 0) {

                val ls = mutable.ListBuffer[Array[HivePartitionPart]]()
//                sql(s"show tables like '*$targetTable$transactionalTmpTableSign$tid'")
//                  .collect()
//                  .foreach{y=>
//                    val tt = y.getAs[String]("tableName")
//                    sql(s"show partitions $tt")
//                      .collect()
//                      .foreach{z=>&&
//                        ls += parseShowPartition(z.getAs[String]("partition"))
//                      }
//                  }
//                val ps = ls.toArray[Array[HivePartitionPart]]

                //val ts =  sql(s"show tables like '*$targetTable$transactionalTmpTableSign$tid'").rdd.map(_.getAs[String]("tableName")).collect()
                //val ps = partitions(ts:_*)

                //前面已经做了删除操作
                LOG.warn(s"HiveClient rollback", s"Reverting to the legacy data, Backup table is empty, Drop partitions of targetTable!! \ntable: ${targetTable} \npartitions: ${partitionsWhereSQL(ps)}")
                //hiveContext.sql(s"truncate table ${targetTable}")
                //partitionsAlterSQL(ps).foreach{y=>
                //  sql(s"alter table ${targetTable} drop if exists partition($y)")
                //}

              }else {
                LOG.warn(s"HiveClient rollback", s"Reverting to the legacy data, Overwrite by backup table: ${t} \nSelect backup table fields:\n ${selects.mkString(", \n")}")

                val df = hiveContext
                  .read
                  .table(t)
                  .selectExpr(selects:_*)

                val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient read backup table repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
//                val parts = sparkPartitionNum("HiveClient read backup table repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)

                df.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
                  .write
                  .format("orc")
                  .mode(SaveMode.Overwrite)
                  .insertInto(targetTable)
              }

            }
          }

// fix bug
        sql(s"show tables like '*${transactionalLegacyDataBackupCompletedTableSign}*'")
//        hiveContext.sql(s"show tables like '*${transactionalLegacyDataBackupCompletedTableSign}*'")
            .collect
            .sortBy{x=>(
              x.getAs[String]("tableName")
                .split(transactionalLegacyDataBackupCompletedTableSign)(1)
                .split(TransactionManager.parentTransactionIdSeparator)(0),
              Integer.parseInt(x.getAs[String]("tableName")
                .split(transactionalLegacyDataBackupCompletedTableSign)(1)
                .split(TransactionManager.parentTransactionIdSeparator)(1))
            )}
            .reverse
            .foreach { x =>

            cleanable.addAction{sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")}
//            sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
////            hiveContext.sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
          }

        sql(s"show tables like '*${transactionalLegacyDataBackupProgressingTableSign}*'")
//        hiveContext.sql(s"show tables like '*${transactionalLegacyDataBackupProgressingTableSign}*'")
          .collect
          .foreach { x =>
            cleanable.addAction{sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")}
//            sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
////            hiveContext.sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
          }

        sql(s"show tables like '*${transactionalTmpTableSign}*'")
//        hiveContext.sql(s"show tables like '*${transactionalTmpTableSign}*'")
          .collect
          .foreach { x =>
            cleanable.addAction{sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")}
//            sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
////            hiveContext.sql(s"drop table ${x.getAs[String]("database")}.${x.getAs[String]("tableName")}")
          }
        return cleanable
      }

      LOG.warn(s"HiveClient rollback started(cookies not empty)")
      val cs = cookies.asInstanceOf[Array[HiveRollbackableTransactionCookie]]
      cs.foreach { x =>
        val parentTid = x.id.split(TransactionManager.parentTransactionIdSeparator)(0)

        //Key code !!
        val needRollback = transactionManager.isActiveButNotAllCommited(parentTid)

        if (needRollback) {
          //Revert to the legacy data!!
          //SaveMode.Overwrite is Overwrite partition

          val bc = hiveContext
            .read
            .table(x.transactionalCompletedBackupTable)
            .count()

          // +
          lTimePartitionsAlterSQL(x.partitions).distinct.foreach{y=>
            sql(s"alter table ${x.targetTable} drop if exists partition($y)")
          }

          if(bc == 0) {
            LOG.warn(s"HiveClient rollback", s"Reverting to the legacy data, Backup table is empty, Drop partitions of targetTable!! \ntable: ${x.transactionalCompletedBackupTable} \npartitions: ${partitionsWhereSQL(x.partitions)}")
            //hiveContext.sql(s"truncate table ${x.targetTable}")
            //            partitionsAlterSQL(x.partitions).foreach{y=>
            //              sql(s"alter table ${x.targetTable} drop if exists partition($y)")
            //            }
          }else {
            LOG.warn(s"HiveClient rollback", s"Reverting to the legacy data, Overwrite by backup table: ${x.transactionalCompletedBackupTable}")
            hiveContext
              .read
              .table(x.transactionalCompletedBackupTable)
              .write
              .format("orc")
              .mode(SaveMode.Overwrite)
              .insertInto(x.targetTable)
          }

        }
      }
      cs.foreach { x =>
        cleanable.addAction{sql(s"drop table if exists ${x.transactionalCompletedBackupTable}")}
        cleanable.addAction{sql(s"drop table if exists ${x.transactionalProgressingBackupTable}")}
        cleanable.addAction{sql(s"drop table if exists ${x.transactionalTmpTable}")}
//        sql(s"drop table if exists ${x.transactionalCompletedBackupTable}")
////        hiveContext.sql(s"drop table if exists ${x.transactionalCompletedBackupTable}")
//        //Delete it if exists
//        sql(s"drop table if exists ${x.transactionalProgressingBackupTable}")
////        hiveContext.sql(s"drop table if exists ${x.transactionalProgressingBackupTable}")
//        sql(s"drop table if exists ${x.transactionalTmpTable}")
////        hiveContext.sql(s"drop table if exists ${x.transactionalTmpTable}")
      }

      LOG.warn(s"HiveClient rollback completed")
      cleanable
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HiveClientException(s"Hive Transaction Rollback Fail, module: $moduleName, cookies: " + cookies, e)
    }
  }

//  private def sparkPartitionNum(logTip: String,shufflePartitions: Int, rddPartitions: Int, hivePartitions: Int): Int = {
//    var result = 0
//    if(rddPartitions == 0 || hivePartitions == 0 || shufflePartitions == 0) {
//      result = 1
//    }else {
//
//      result = hivePartitions*2
//    }
//
//    LOG.warn(logTip, "count", result, "shufflePartitions", shufflePartitions, "rddPartitions", rddPartitions, "hivePartition", hivePartitions)
//    result
//
//  }

  def aHivePartitionRecommendedFileNumber(logTip: String, shufflePartitions: Int, rddPartitions: Int, hivePartitions: Int): Int ={
    var result = 0
    if(rddPartitions == 0 || hivePartitions == 0) {
      result = 8
    }else {
      result = 8//Math.ceil(1.0*shufflePartitions/hivePartitions).toInt
    }

    LOG.warn(logTip, "aHivePartitionRecommendedFileNumber", result, "shufflePartitions", shufflePartitions, "rddPartitions", rddPartitions, "hivePartition", hivePartitions)
//    Math.ceil(hivePartitions).toInt
    result
  }

  def sql(sqlText: String): DataFrame ={
    LOG.warn("Execute HQL", sqlText)

    // Fix bug: https://issues.apache.org/jira/browse/SPARK-22686
    var level: Level = null;
    var log: org.apache.log4j.Logger = null
    if(sqlText.contains("if exists")) {
      level = org.apache.log4j.Logger.getLogger(classOf[DropTableCommand]).getLevel
      log = org.apache.log4j.Logger.getLogger(classOf[DropTableCommand])
      log.setLevel(Level.ERROR)
    }

    var result = hiveContext.sql(sqlText)

    // Fix bug: https://issues.apache.org/jira/browse/SPARK-22686
    if(sqlText.contains("if exists")) {
      log.setLevel(level)
    }
    result
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    try {
      cookies.foreach{x=>
        if(x.isInstanceOf[HiveRollbackableTransactionCookie]) {
          var c = x.asInstanceOf[HiveRollbackableTransactionCookie]
          sql(s"drop table if exists ${c.transactionalCompletedBackupTable}")
          sql(s"drop table if exists ${c.transactionalProgressingBackupTable}")
          sql(s"drop table if exists ${c.transactionalTmpTable}")
        }
      }
    } catch {
      case e: Exception =>
//        try {
//          init()
//        }
        throw new HiveClientException(s"Hive Transaction Clean Fail, module: $moduleName, cookie: " + OM.toJOSN(cookies), e)
    }
  }
}



object HiveClient{

  private var  lock = new Object()
  private var executorService = ExecutorServiceUtil.createdExecutorService(1)// must is 1
}