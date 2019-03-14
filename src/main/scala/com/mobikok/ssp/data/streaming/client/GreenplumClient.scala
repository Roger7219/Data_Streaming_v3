package com.mobikok.ssp.data.streaming.client

import java.io.FileNotFoundException
import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.mobikok.message.client.MessageClient
import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.ssp.data.streaming.client.cookie.{HiveRollbackableTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.HiveClientException
import com.mobikok.ssp.data.streaming.util._
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.math.Ordering

/**
  * Created by Administrator on 2017/6/8.
  */
class GreenplumClient (moduleName:String, config: Config, ssc: StreamingContext, messageClient: MessageClient, transactionManager: MixTransactionManager, moduleTracer: ModuleTracer) {

  private val LOG: Logger = new Logger(moduleName, getClass.getName, new Date().getTime)
  private val sqlContext = new SQLContext(ssc.sparkContext)
  private val hiveContext = new HiveContext(ssc.sparkContext)


  private  val greenplumJDBCClient:GreenplumJDBCClient = new GreenplumJDBCClient(
    moduleName,
    "jdbc:postgresql://node15:5432/postgres?reWriteBatchedInserts=true&autosave=CONSERVATIVE",
    "gpadmin",
    "gpadmin"
  )

  Class.forName("org.postgresql.Driver")
  private val options = Map(
    "url" -> "jdbc:postgresql://node15:5432/postgres?reWriteBatchedInserts=true&autosave=CONSERVATIVE",
    "user" -> "gpadmin",
    "password" -> "gpadmin",
    "driver" -> "org.postgresql.Driver"
  )

  def overwrite (greenplumTable: String, hiveTable: String) {
    overwrite(greenplumTable, hiveTable, s"${greenplumTable}_default_partition", "exchange_partition", "-")
  }

  def overwrite (greenplumTable: String, hiveTable: String, greenplumPartitionTable: String) {
    overwrite(greenplumTable, hiveTable, greenplumPartitionTable, "exchange_partition", "-")
  }

  def overwrite (greenplumTable: String, hiveTable: String, greenplumPartitionTable: String, partitionField: String, partitionValue:Object) {
    try {
      // eg: string: '123', int: 123
      val defV = OM.toJOSN(partitionValue).replaceAll("\"", "'")

      val df = hiveContext.read.table(hiveTable).select(col("*"), expr(s"$defV as $partitionField")).persist();
      val c = df.count()
      val pTmpTable = s"${greenplumPartitionTable}_tmp";

      try {
                                                                                                                                                               greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pTmpTable}")
      }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}
      LOG.warn(s"GreenplumClient clean prev pluggable table if exists", pTmpTable)

      try {
        greenplumJDBCClient.execute(s"CREATE TABLE ${pTmpTable} (LIKE $greenplumPartitionTable)")
      }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}

      LOG.warn(s"GreenplumClient write pluggable table starting", s"tmpTable: $pTmpTable\ncount: $c")

      var p = (c/40000).toInt
      if(p == 0) p = 1
      if(p > 200) p = 200

      df.repartition(p).write.mode(SaveMode.Append).format("jdbc").options(options).option("dbtable", pTmpTable).save()
      LOG.warn(s"GreenplumClient write pluggable table completed", s"partitions: $p")

      greenplumJDBCClient.execute(
        s"""
           |  ALTER TABLE $greenplumTable
           |  EXCHANGE PARTITION FOR ($defV)
           |  WITH TABLE $pTmpTable
             """.stripMargin)
      LOG.warn(s"GreenplumClient exchange partition", s"targetTable: $greenplumTable\ntmpTable: $pTmpTable\npartition: ${partitionValue}")

      greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pTmpTable}")
      LOG.warn(s"GreenplumClient drop pluggable table if exists ", pTmpTable)

      df.unpersist()
    }catch {case e:FileNotFoundException=>
        throw e;
//      overwrite (greenplumTable, hiveTable, greenplumPartitionTable, partitionField, partitionValue)
    }
  }

  def overwrite (greenplumDwrTable: String, hiveDwrTable: String, hivePartitions: Array[HivePartitionPart], hivePartitionField: String) {

    try {

      LOG.warn(s"GreenplumClient overwrite started", s"greenplumDwrTable: $greenplumDwrTable\nhiveDwrTable: $hiveDwrTable\nparts:${util.Arrays.deepToString(hivePartitions.asInstanceOf[Array[Object]])}\npartitionField:$hivePartitionField" )

      hivePartitions
        .map{x=>
          (
            x.value,
            x.value.replaceAll("-", "_"),
            hiveContext.read.table(hiveDwrTable).where(s"$hivePartitionField = '${x.value}'").persist()
          )
        }
        .foreach{x=>
          val pValue = x._1
          val pName = x._2
          val pTable = s"${greenplumDwrTable}_1_prt_${x._2}"
          val pTmpTable = s"${greenplumDwrTable}_1_prt_${x._2}_tmp"
          val df = x._3
          val c = df.count()
          try {
            greenplumJDBCClient.execute(
              s"""
                 |  ALTER TABLE $greenplumDwrTable
                 |  ADD PARTITION "$pName"
                 |  VALUES ('$pValue')
               """.stripMargin)
          }catch {case e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}
          LOG.warn(s"GreenplumClient added partition if not exists", pValue)

          try {
            greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pTmpTable}")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}
          LOG.warn(s"GreenplumClient clean prev pluggable table if exists", pTmpTable)

          try {
            greenplumJDBCClient.execute(s"CREATE TABLE ${pTmpTable} (LIKE $pTable)")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}

          LOG.warn(s"GreenplumClient write pluggable table starting", s"tmpTable: $pTmpTable\ncount: $c")

          var p = (c/40000).toInt
          if(p == 0) p = 1
          if(p > 200) p = 200


          df.repartition(p).write.mode(SaveMode.Append).format("jdbc").options(options).option("dbtable", pTmpTable).save()
          LOG.warn(s"GreenplumClient write pluggable table completed", s"partitions: $p")

          greenplumJDBCClient.execute(
            s"""
               |  ALTER TABLE $greenplumDwrTable
               |  EXCHANGE PARTITION FOR ('$pValue')
               |  WITH TABLE $pTmpTable
             """.stripMargin)
          LOG.warn(s"GreenplumClient exchange partition", s"targetTable: $greenplumDwrTable\ntmpTable: $pTmpTable\npartition: ${pValue}")

          greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pTmpTable}")
          LOG.warn(s"GreenplumClient drop pluggable table if exists", pTmpTable)

          x._3.unpersist()
        }

      //容错处理
    } catch {case e:FileNotFoundException =>
      throw e;
//      overwrite (greenplumDwrTable, hiveDwrTable, hivePartitions, hivePartitionField)
    }
  }

  def upsert (greenplumTable: String, newDF: DataFrame, onConflictFields: Array[String], partitions: Array[HivePartitionPart], partitionField: String) {
    try {

      LOG.warn(s"GreenplumClient upsert started", s"partitions:${util.Arrays.deepToString(partitions.asInstanceOf[Array[Object]])}\npartitionField:$partitionField\nonConflictFields:${util.Arrays.deepToString(onConflictFields.asInstanceOf[Array[Object]])}" )

      partitions
        .map{x=>
          (
            x.value,
            x.value.replaceAll("-", "_")
  //          hiveContext.read.table(hiveTable).where(s"$hivePartitionField = '${x.value}'")
          )
        }
        .foreach{x=>
          val pValue = x._1
          val pName = x._2
          val pTable = s"${greenplumTable}_1_prt_${x._2}"
          val pCloneTable = s"${greenplumTable}_1_prt_${x._2}_4_clone"
          val pUpsertTable = s"${greenplumTable}_1_prt_${x._2}_4_upsert"
  //        val df = x._3
          newDF.persist()
          val c = newDF.count()
          try {
            greenplumJDBCClient.execute(
              s"""
                 |  ALTER TABLE $greenplumTable
                 |  ADD PARTITION "$pName"
                 |  VALUES ('$pValue')
               """.stripMargin)
          }catch {case e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}
          LOG.warn(s"GreenplumClient added partition if not exists", pValue)

          try {
            greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pUpsertTable}")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}
          LOG.warn(s"GreenplumClient clean prev table(for newDF upsert) if exists", pUpsertTable)

          try {
            greenplumJDBCClient.execute(s"CREATE TABLE ${pUpsertTable} (LIKE $pTable)")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}

          moduleTracer.trace("  greenplumn ready")

          var p = (c/40000).toInt
          if(p == 0) p = 1
          if(p > 200) p = 200

          LOG.warn(s"GreenplumClient write newDF to upsert table starting", s"upsertTable: $pUpsertTable\ncount: $c\nrepartition:$p")

          newDF.where(s"""b_date = "$pValue" """).repartition(p).write.mode(SaveMode.Append).format("jdbc").options(options).option("dbtable", pUpsertTable).save()
          LOG.warn(s"GreenplumClient write newDF to upsert table completed", s"partitions: $p")

          moduleTracer.trace("  greenplumn write newDF")

          try {
            greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pCloneTable} ")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}

          try {
            greenplumJDBCClient.execute(s"CREATE TABLE ${pCloneTable} (LIKE $pTable)")
          }catch {case  e:Exception=> LOG.warn("Greenplum execute fail", e.getMessage)}

          greenplumJDBCClient.execute(s"INSERT INTO ${pCloneTable} SELECT * FROM $pTable")
          LOG.warn(s"GreenplumClient clone table completed")

          moduleTracer.trace("  greenplumn clone table")

          if(onConflictFields != null && onConflictFields.length > 0) {
            greenplumJDBCClient.execute(
              s"""
                 |  DELETE FROM $pCloneTable a
                 |  WHERE EXISTS (
                 |    SELECT 1 FROM $pUpsertTable b
                 |    WHERE ${onConflictFields.map{x=> s"a.$x = b.$x" }.mkString(" AND ")}
                 |  )
               """.stripMargin)
            LOG.warn(s"GreenplumClient delete clone table by upsert table", s"targetTable: $greenplumTable\ntmpTable: $pUpsertTable\npartition: ${pValue}")
            moduleTracer.trace("  greenplumn delete clone table of newDF ")
          }

          greenplumJDBCClient.execute(s"INSERT INTO ${pCloneTable} SELECT * FROM $pUpsertTable")
          LOG.warn(s"GreenplumClient insert clone table by upsert table completed")

          moduleTracer.trace("  greenplumn insert clone table of newDF")

          greenplumJDBCClient.execute(
            s"""
               |  ALTER TABLE $greenplumTable
               |  EXCHANGE PARTITION FOR ('$pValue')
               |  WITH TABLE $pCloneTable
             """.stripMargin)
          LOG.warn(s"GreenplumClient exchange partition", s"targetTable: $greenplumTable\ncloneTable: $pCloneTable\npartition: ${pValue}")

          moduleTracer.trace("  greenplumn exchange")

          greenplumJDBCClient.execute(s"DROP TABLE IF EXISTS ${pUpsertTable}")
          LOG.warn(s"GreenplumClient drop pluggable table if exists", pUpsertTable)

          newDF.unpersist()
        }

    }catch { case e:FileNotFoundException=>
      throw e;
//      upsert (greenplumTable, newDF, onConflictFields, partitions, partitionField)
    }
  }


}

