package com.mobikok.ssp.data.streaming.client

import java.net.URLDecoder
import java.util.{Date, List}

import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.ssp.data.streaming.client.cookie._
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.HiveClientException
import com.mobikok.ssp.data.streaming.transaction.{TransactionCookie, TransactionManager, TransactionRoolbackedCleanable, TransactionalClient}
import com.mobikok.ssp.data.streaming.udf.HiveContextCreator
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.log4j.Level
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering

/**
  * Created by Administrator on 2017/6/8.
  */
class HiveClient(moduleName:String, config: Config, ssc: StreamingContext, messageClient: MessageClient, transactionManager: TransactionManager, moduleTracer: ModuleTracer) extends TransactionalClient {

  //private[this] val LOG = Logger.getLogger(getClass().getName());

  private var hiveJDBCClient: HiveJDBCClient = null
  var hiveContext: HiveContext = HiveContextCreator.create(ssc.sparkContext)
  var compactionHiveContext:HiveContext = HiveContextCreator.create(ssc.sparkContext)
  var hdfsUtil: HdfsUtil = null

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"
  val shufflePartitions = config.getInt("spark.conf.set.spark.sql.shuffle.partitions")

  //  private val transactionalPartitionField = "tid"
  val LOG: Logger = new Logger(moduleName, getClass, new Date().getTime)

  override def init(): Unit = {
    LOG.warn(s"HiveClient init started")
//    hiveContext = HiveContextGenerater.generate(ssc.sparkContext)
    LOG.warn("show tables: ", hiveContext.sql("show tables").collect())

//    compactionHiveContext = HiveContextGenerater.generate(ssc.sparkContext)

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
      moduleTracer.trace(s"        into repartition rs: ${df.rdd.getNumPartitions}, ps: ${ps.length}, fs: $fileNumber")
      moduleTracer.trace(s"${makeShowPartition(ps).mkString("\n        ", "\n        ", "")}")
      LOG.warn("Into partitions", makeShowPartition(ps).mkString("\n"))

      if(!transactionManager.needRealTransactionalAction()) {

        df.selectExpr(fs:_*)
          .coalesce(fileNumber)
          //.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Append)
          .insertInto(table)
        return new HiveNonTransactionCookie(transactionParentId, tid, table, SaveMode.Append, ps)
      }

      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

//      createTableIfNotExists(tt, table)
//
//      df.selectExpr(fs:_*)
//        .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
//        .write
//        .format("orc")
//        .mode(SaveMode.Append)
//        .insertInto(tt)

      df.selectExpr(fs:_*)
        .createOrReplaceTempView(tt)

      moduleTracer.trace("        into table done")

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

  def into(transactionParentId: String, table: String, df: DataFrame, partitionField: String, partitionFields: String*): HiveTransactionCookie = {
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

    moduleTracer.trace("        count partitions")
    into(transactionParentId, table, df, ps)
  }


  def overwriteUnionSum (transactionParentId: String,
                         table: String,
                         newDF: DataFrame,
                         aggExprsAlias: List[String],
                         unionAggExprsAndAlias: List[Column],
                         overwriteAggFields: Set[String], // 已弃用，待删
                         groupByFields: Array[String],
                         beforeWriteCallback: DataFrame => DataFrame,
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
    moduleTracer.trace("        count partitions")

    overwriteUnionSum(transactionParentId, table, newDF, aggExprsAlias, unionAggExprsAndAlias, overwriteAggFields, groupByFields, ps, beforeWriteCallback, partitionField, partitionFields:_*)
  }

  // groupByExprsAlias: 不包含分区字段
  def overwriteUnionSum (transactionParentId: String,
                         table: String,
                         newDF: DataFrame,
                         aggExprsAlias: List[String],
                         unionAggExprsAndAlias: List[Column],
                         overwriteAggFields: Set[String],
                         groupByFields: Array[String],
                         ps: Array[Array[HivePartitionPart]],
                         beforeWriteCallback: DataFrame => DataFrame,
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

      // 不读取OVERWIRTE_FIXED_L_TIME中的数据
      var rps = ps.filter{x=>
          x.filter{y=>
            "l_time".equals(y.name) && HiveClient.OVERWIRTE_FIXED_L_TIME.equals(y.value)
          }.isEmpty
        }

      var w = partitionsWhereSQL(ps)   // (l_time="2018-xx-xx xx:00:00" and b_date="2018-xx-xx" and b_time="2018-xx-xx xx:00:00")
      var rw = partitionsWhereSQL(rps) // 不含l_time=0001-01-01 00:00:00的分区

      LOG.warn(s"HiveClient overwriteUnionSum", "overwrite partitions", w, "read original partitions", rw)

      val isE = newDF.take(1).isEmpty

      if(isE) {

        LOG.warn(s"Hive overwriteUnionSum skipped, Because no data needs to be written !!")

      }else {
        val original = hiveContext
          .read
          .table(table)
          .where(rw)

        moduleTracer.trace("        read hive data for union")

        // Group by fields with partition fields
        val gs = (groupByFields :+ partitionField) ++ partitionFields

        var fs = original.schema.fieldNames
        var _newDF = newDF.select(fs.head, fs.tail:_*)

        //忽略大小写
//        var _overwriteAggFields = overwriteAggFields.map{x=>x.toLowerCase.trim}
//        var os = fs.map{x=>if(_overwriteAggFields.contains(x.toLowerCase())) expr(s"null as $x") else expr(x)};
          var os = fs.map{x=>expr(s"`$x`")}
        //      LOG.warn(s"HiveClient re-ordered field name via hive table _newDF take(2)", _newDF.take(2))

        if(beforeWriteCallback != null) _newDF = beforeWriteCallback(_newDF)

        // Sum
        var updated = original
          .select(os:_*)
          .union(_newDF /*newDF*/)
          .groupBy(gs.head, gs.tail:_*)
          .agg(
            unionAggExprsAndAlias.head,
            unionAggExprsAndAlias.tail:_*
          )
          .select(fs.head, fs.tail:_* /*groupByFields ++ aggExprsAlias ++ ts:_**/)

        val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient unionSum repartition", shufflePartitions, updated.rdd.getNumPartitions, ps.length)
  //      val parts = sparkPartitionNum("HiveClient unionSum repartition", shufflePartitions, updated.rdd.getNumPartitions, ps.length)
        moduleTracer.trace(s"        union sum repartition rs: ${updated.rdd.getNumPartitions}, ps: ${ps.length}, fs: ${fileNumber}")
        moduleTracer.trace(s"${makeShowPartition(ps).mkString("\n        ", "\n        ", "")}")
        LOG.warn("Union sum partitions", makeShowPartition(ps).mkString("\n"))

        if(transactionManager.needRealTransactionalAction()) {
          //支持事务，先写入临时表，commit()时在写入目标表
//          createTableIfNotExists(tt, table)
//          updated
//  //          .coalesce(1)
//            .repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
//            .write
//            .format("orc")
//            .mode(SaveMode.Overwrite)
//            .insertInto(tt)
            updated
                .createOrReplaceTempView(tt)



        }else {
          //非事务，直接写入目标表
          updated
            .coalesce(fileNumber)
            //.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
            .write
            .format("orc")
            .mode(SaveMode.Overwrite)
            .insertInto(table)


        }
        moduleTracer.trace("        union sum and write")
      }

      if(transactionManager.needRealTransactionalAction()) {
        return new HiveRollbackableTransactionCookie(
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
      } else {
        return new HiveNonTransactionCookie(transactionParentId, tid, table, SaveMode.Overwrite, ps)
      }

    } catch {
      case e: Exception =>
        LOG.warn(ExceptionUtil.getStackTraceMessage(e))
        throw new HiveClientException(s"Hive Insert Overwrite Fail, module: $moduleName, transactionId:  $tid", e)
    }
  }

  def overwrite(transactionParentId: String, table: String, isOverwirteFixedLTime: Boolean, df: DataFrame, partitionField: String, partitionFields: String*): HiveTransactionCookie = {

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

    overwrite(transactionParentId, table, isOverwirteFixedLTime, df, ps)
  }

  def overwrite(transactionParentId: String, table: String, isOverwirteFixedLTime: Boolean, df: DataFrame, ps: Array[Array[HivePartitionPart]]): HiveTransactionCookie = {

    val fs = hiveContext.read.table(table).schema.fieldNames
    var tid: String = null
    try {

      // Ensure l_time=0001-01-01 00:00:00 if isOverwirteFixedLTime=true
      if(isOverwirteFixedLTime) {
        ps.foreach{x=>
          x.foreach { y =>
            if("l_time".equals(y.name) && !HiveClient.OVERWIRTE_FIXED_L_TIME.equals(y.value)){
              throw new RuntimeException(s"l_time must be: ${HiveClient.OVERWIRTE_FIXED_L_TIME} if module config: overwrite=true"  )
            }
          }
        }
      }

      tid = transactionManager.generateTransactionId(transactionParentId)

      val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient overwrite repartition", shufflePartitions, df.rdd.getNumPartitions, ps.length)
      moduleTracer.trace(s"        overwrite repartition fs: $fileNumber, ps: ${ps.length}, rs: ${df.rdd.getNumPartitions}")
      moduleTracer.trace(s"            ${makeShowPartition(ps).mkString("\n        ", "\n        ", "")}")
      LOG.warn("Overwrite partitions", makeShowPartition(ps).mkString("\n"))

      if(!transactionManager.needRealTransactionalAction()) {
        df.selectExpr(fs:_*)
          .coalesce(fileNumber)
          //.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(table)
        return new HiveNonTransactionCookie(transactionParentId, tid, table, SaveMode.Overwrite, ps)
      }

      val tt = table + transactionalTmpTableSign + tid
      val pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      val ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

      if(!df.take(1).isEmpty) {
//        createTableIfNotExists(tt, table)
      }

      if(!df.take(1).isEmpty) {
//        df.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
//          .write
//          .format("orc")
//          .mode(SaveMode.Overwrite)
//          .insertInto(tt)

        df.selectExpr(fs:_*)
          .createOrReplaceTempView(tt);
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
    }.mkString( " or ")
    if ("".equals(w)) w = "'no_partition_specified' <> 'no_partition_specified'"
    "( " + w + " )"
  }

  def partitionsAlterSQL(ps : Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString(", ")
    }
  }

  // 将分区信息转成DF格式
  // 目前仅支持b_time、b_date和l_time分区作为Dataframe列名
  def partitionsAsDataFrame(ps : Array[Array[HivePartitionPart]]): DataFrame ={
    var rows = ps.map{x=>
      val row = new Array[String](3)
      x.map{y=>
        y.name match {
          case "b_time" => row(0) = y.value
          case "b_date" => row(1) = y.value
          case "l_time" => row(2) = y.value
        }
      }
      (row(0), row(1), row(2))
    }.toSeq

    hiveContext
      .createDataFrame(rows)
      .toDF("b_time", "b_date", "l_time")
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

    // 之前的写入非真正的事务操作，是直接写入了目标表，跳过
    if(cookie.isInstanceOf[HiveNonTransactionCookie]) {
      return
    }

    try {
      val c = cookie.asInstanceOf[HiveRollbackableTransactionCookie]

      LOG.warn(s"Hive commit start, cookie: ${OM.toJOSN(cookie)}")

      if(c.isEmptyData) {
        LOG.warn(s"Hive commit skipped, Because no data is inserted (into or overwrite) !!")
      }
      else {
        //Back up legacy data for possible rollback
        createTableIfNotExists(c.transactionalProgressingBackupTable, c.targetTable)

        var lts = c.partitions.map{x=>x.filter(y=>"l_time".equals(y.name) && !y.value.equals(HiveClient.OVERWIRTE_FIXED_L_TIME) )}.filter{x=>x.nonEmpty}

        //正常情况下只会有一个l_time值
        val lt = lts.flatMap{x=>x}.toSet
        if(lt.size > 1){
          throw new HiveClientException(s"l_time must be only one value for backup, But has ${lt.size} values: ${lt} !")
        }

        var w  = partitionsWhereSQL(lts)//partitionsWhereSQL(c.partitions)

        LOG.warn(s"Hive before commit backup table ${c.targetTable} where: ", lts)

        //For transactional rollback overwrite original empty partitions
        val tranPs = c.partitions.filter{x=>x.filter(y=> ("l_time".equals(y.name) && y.value.equals(HiveClient.OVERWIRTE_FIXED_L_TIME))).isEmpty}.filter{x=>x.nonEmpty}
        partitionsAlterSQL(tranPs).foreach{x=>
          sql(s"alter table ${c.transactionalProgressingBackupTable} add partition($x)")
        }

        var backPs = if(lt.isEmpty) {
          Array[String]()
        }else {
          sql(s"show partitions ${c.targetTable} partition(${lt.head.name}='${lt.head.value}')")
              .collect()
              .map{x=>x.getAs[String]("partition")}
        }
        val backPss = backPs.length

        val df = hiveContext
          .read
          .table(c.targetTable)
          .where(w) //key code !!
//          .repartition(shufflePartitions) //?

        moduleTracer.trace(s"        read for backup repartition start")

        //hivePartitions不一定是df.rdd.getNumPartitions, 待优化
        val fileNumber = aHivePartitionRecommendedFileNumber("HiveClient read for backup repartition", shufflePartitions, df.rdd.getNumPartitions, backPss)
//        val parts = sparkPartitionNum("HiveClient read for backup repartition", shufflePartitions, df.rdd.getNumPartitions, df.rdd.getNumPartitions)
        moduleTracer.trace(s"        read for backup rs: ${df.rdd.getNumPartitions}, ps: ${backPss}, fs: $fileNumber")
        moduleTracer.trace(s"            ${backPs.mkString("\n        ", "\n        ", "")}")
        LOG.warn("Read for backup partitions", backPs.mkString("\n"))

        // 待优化，文件直接复制
        df//.coalesce(fileNumber)
          //.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
          .write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .insertInto(c.transactionalProgressingBackupTable)

        moduleTracer.trace("        insert into progressing backup table")
        sql(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")
        moduleTracer.trace("        rename to completed backup table")

        //Commit critical code
        val df2 = hiveContext
          .read
          .table(c.transactionalTmpTable)

        val fileNumber2 = aHivePartitionRecommendedFileNumber("HiveClient read table repartition", shufflePartitions, df2.rdd.getNumPartitions, c.partitions.length)
//        val parts2 = sparkPartitionNum("HiveClient read pluggable table repartition", shufflePartitions, df2.rdd.getNumPartitions, c.partitions.length)
        moduleTracer.trace(s"        read transactional tmp table rs: ${df2.rdd.getNumPartitions}, fs: $fileNumber2, ps: ${c.partitions.length}")
        moduleTracer.trace(s"            ${makeShowPartition(c.partitions).mkString("\n        ", "\n        ", "")}")
        LOG.warn(s"Read transactional tmp table partitions", makeShowPartition(c.partitions).mkString("\n"))

        df2.coalesce(fileNumber2)
          //.repartition(fileNumber2*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber2}) ) )"))
          .write
          .format("orc")
          .mode(c.saveMode)
          .insertInto(c.targetTable)

          moduleTracer.trace("        insert into target table")
      }

//      tryAsyncCompaction(c)

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
    messageClient.messageClientApi.pushMessage(ms:_*)

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
    var lts = messageClient.messageClientApi
      .pullMessage(new MessagePullReq(cons, Array(topic) ))
      .getPageData

    if(lts.isEmpty) {
      return
    }

    lts = lts
      .sortBy{_.getKeyBody}(Ordering.String.reverse)
      .tail
// 取两个l_time之前的，尽量避免与流统计操作的数据发生并发操作冲突
//    if(lts.nonEmpty) lts = lts.tail
    lts = lts.sortBy{_.getOffset}

    LOG.warn(s"Hive compact table ${c.targetTable} starting, partitions", lts.toArray)

    val tt = s"${c.targetTable}${compactingTmpTableSign}"
    val compactedTt = s"${c.targetTable}$compactedTmpTableSign"

    lts.foreach{x=>

//      sql(s"create table if not exists $tt like ${c.targetTable}")
      createTableIfNotExists(tt, c.targetTable)

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
      messageClient.messageClientApi.commitMessageConsumer(new MessageConsumerCommitReq(cons, topic, x.getOffset))

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

    RunAgainIfError.run({
      HiveClient.lock.synchronized{

        val compactedSuffix = ".compacted"

        sql(s"show tables like '*$compactedTmpTableSign' ")
          .collect()
          .foreach{t =>
            val srcT = t.getAs[String]("tableName")

            val dstT = s"""${srcT.substring(0,srcT.indexOf(compactedTmpTableSign))}"""

            LOG.warn("Hive compact: ", "exsist pluggable table ", srcT, "dstT ", dstT)

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
    }, "HiveClient.tryReMoveCompactResultTempFile() error! ")

  }

  def makeShowPartition(ps : Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map{x=>
      x.map{y=>
        s"${/*URLEncoder.encode(*/String.valueOf(y.name)/*, "utf-8")*/}=${/*URLEncoder.encode(*/String.valueOf(y.value)/*, "utf-8")*/}"
      }.mkString("/")
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
  override def rollback (cookies: TransactionCookie*): TransactionRoolbackedCleanable = {

    try {
      moduleTracer.trace("hive try rollback start")
      val cleanable = new TransactionRoolbackedCleanable()
      if (cookies.isEmpty) {
        LOG.warn(s"HiveClient rollback started(non specified cookie)")
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
            val needRollback = transactionManager.isActiveButNotAllCommitted(parentTid)

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

                df.coalesce(fileNumber)
                  //.repartition(fileNumber*2, expr(s"concat_ws('^', b_date, b_time, l_time, ceil( rand() * ceil(${fileNumber}) ) )"))
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

      }else{

        LOG.warn(s"HiveClient rollback started(specified cookie)")
        val cs = cookies.asInstanceOf[Array[HiveRollbackableTransactionCookie]]
        cs.foreach { x =>
          val parentTid = x.id.split(TransactionManager.parentTransactionIdSeparator)(0)

          //Key code !!
          val needRollback = transactionManager.isActiveButNotAllCommitted(parentTid)

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
        }

      }

      moduleTracer.trace("hive try rollback done")
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
      result = 10 //Math.ceil(8.0/hivePartitions).toInt
    }else {
      result = 10;//Math.ceil(8.0/hivePartitions).toInt //Math.ceil(1.0*shufflePartitions/hivePartitions).toInt
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

  def emptyDF(schema: StructType): DataFrame ={
    hiveContext.createDataFrame(hiveContext.sparkContext.emptyRDD[Row], schema)
  }

  def emptyDF(tableName: String): DataFrame ={
    hiveContext.createDataFrame(
      hiveContext.sparkContext.emptyRDD[Row],
      hiveContext.read.table(tableName).schema
    )
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    try {
      cookies.foreach{x=>
        if(x.isInstanceOf[HiveRollbackableTransactionCookie]) {
          var c = x.asInstanceOf[HiveRollbackableTransactionCookie]
          sql(s"drop table if exists ${c.transactionalCompletedBackupTable}")
          sql(s"drop table if exists ${c.transactionalProgressingBackupTable}")
          sql(s"drop table if exists ${c.transactionalTmpTable}")
          sql(s"drop view if exists ${c.transactionalCompletedBackupTable}")
          sql(s"drop view if exists ${c.transactionalProgressingBackupTable}")
          sql(s"drop view if exists ${c.transactionalTmpTable}")
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

  val OVERWIRTE_FIXED_L_TIME = "0001-01-01 00:00:00"
  private var  lock = new Object()
  private var executorService = ExecutorServiceUtil.createdExecutorService(1)// must is 1

  def main(args: Array[String]): Unit = {
    var list = Array("ss")
    println( list.tail.foreach({x=>}))
    println( list.tail.isEmpty)

  }
}