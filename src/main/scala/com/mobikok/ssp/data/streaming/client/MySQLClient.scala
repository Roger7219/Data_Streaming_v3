package com.mobikok.ssp.data.streaming.client

import java.sql.{DriverManager, ResultSet}
import java.util.List

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client.cookie.{MySQLNonTransactionCookie, MySQLRollbackableTransactionCookie, MySQLTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.MySQLJDBCClientException
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util.{Logger, ModuleTracer, MySqlJDBCClientV2, OM}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._

import scala.collection.JavaConversions._

class MySQLClient(moduleName: String, sc: SparkContext, config: Config, messageClient: MessageClient, transactionManager: TransactionManager, moduleTracer: ModuleTracer) extends Transactional {

  val LOG = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  var mySqlJDBCClient: MySqlJDBCClientV2 = _

  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_backup_progressing_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_backup_completed_"

  override def init(): Unit = {
    mySqlJDBCClient = new MySqlJDBCClientV2(moduleName, config.getString("rdb.url"), config.getString("rdb.user"), config.getString("rdb.password"))
  }

  def createTableIfNotExist(table: String, like: String): Unit = {
    mySqlJDBCClient.execute(s"create table if not exists $table like $like")
  }

  def overwriteUnionSum(transactionParentId: String,
                        table: String,
                        newDF: DataFrame,
                        aggExprsAlias: List[String],
                        //                        unionAggExprsAndAlias: List[Column],
                        groupByFields: Array[String]): Unit = {
    //    var ts =
  }

  def overwriteUnionSum(transactionParentId: String,
                        table: String,
                        newDF: DataFrame,
                        aggExprsAlias: List[String],
                        unionAggExprsAndAlias: List[Column],
                        groupByFields: Array[String],
                        ps: Array[Array[HivePartitionPart]],
                        partitionField: String,
                        partitionFields: String*): MySQLTransactionCookie = {
    var tid: String = null
    LOG.warn(s"MySQLClient overwriteUnionSum start")
    try {

      val sqlContext = new SQLContext(sc)

      tid = transactionManager.generateTransactionId(transactionParentId)
      var tt = table + transactionalTmpTableSign + tid
      var pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      var ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

      val ts = Array(partitionField) ++ partitionFields

      val w = partitionsWhereSQL(ps)
      LOG.warn(s"HiveClient overwriteUnionSum overwrite partitions where", w)

      //      val ts = Array("l_time", "b_date", "b_time")

      val fields = config.getConfigList(s"modules.$moduleName.dwr.mysql.fields").toList
      //      val fieldsMap = newDF.schema.fields.map{ x => x.name -> x.dataType}.toMap

      //      val original = mySqlJDBCClient.executeQuery(s"""SELECT ${fields.mkString(",")} FROM $table WHERE $w""", new Callback[Seq[Row]] {
      //        override def onCallback(rs: ResultSet): Seq[Row] = {
      //          rs.getObject("")
      //
      //        }
      //      })

      val original = sqlContext.read.format("jdbc")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("url", config.getString("rdb.url"))
        .option("user", config.getString("rdb.user"))
        .option("password", config.getString("rdb.password"))
        .option("dbtable", table)
        .load()


      val gs = (groupByFields :+ partitionField) ++ partitionFields

      var fs = original.schema.fieldNames

      var _newDF = newDF.select(fs.head, fs.tail: _*)

      val updated = original.union(newDF)
        .groupBy(gs.head, gs.tail: _*)
        .agg(
          unionAggExprsAndAlias.head,
          unionAggExprsAndAlias.tail: _*
        )
        .select(fs.head, fs.tail: _*)
        .collect()


      if (transactionManager.needTransactionalAction()) {
        createTableIfNotExist(tt, table)
        //        sqlContext.table(table).write.mode(SaveMode.Overwrite).insertInto(table)
        val sqls = updated.map { data =>
          s"""INSERT INTO $table VALUES(${data.mkString(",")})"""
        }
        mySqlJDBCClient.executeBatch(sqls)
      } else {
        //        sqlContext.table(table).write.mode(SaveMode.Overwrite).insertInto(table)
        val sqls = updated.map { data =>
          s"""INSERT INTO $table VALUES(${data.mkString(",")})"""
        }
        mySqlJDBCClient.executeBatch(sqls)
        return new MySQLNonTransactionCookie(transactionParentId, tid, table)
      }

      val isEmpty = newDF.take(1).isEmpty
      moduleTracer.trace("     mysql union sum and write")

      new MySQLRollbackableTransactionCookie(transactionParentId, tid, tt, table, ps, pt, ct, isEmpty)
    } catch {
      case e: Exception =>
        throw new MySQLJDBCClientException(s"MySQL insert update failed, module: $moduleName, transactionId: $tid", e)
    }
  }

  private def partitionsWhereSQL(ps: Array[Array[HivePartitionPart]]): String = {
    var w = ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString("(", " and ", ")")
    }.mkString(" or ")
    if ("".equals(w)) w = "1 = 1"
    w
  }

  override def commit(cookie: TransactionCookie): Unit = {
    if (cookie.isInstanceOf[MySQLNonTransactionCookie]) {
      return
    }
    try {
      var c = cookie.asInstanceOf[MySQLRollbackableTransactionCookie]

      LOG.warn("MySQL before commit starting")

      if (c.isEmptyData) {
        LOG.warn(s"MySQL commit skiped, Because no data is inserted (into or overwrite) !!")
      } else {
        createTableIfNotExist(c.transactionalProgressingBackupTable, c.targetTable)

        var bw = c.partitions.map{ x => x.filter(y => "l_time".equals(y.name))}


      }
    }

  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    null
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    try {
      cookies.foreach{ x=>
        if (x.isInstanceOf[MySQLRollbackableTransactionCookie]) {
          mySqlJDBCClient.execute(s"drop table if exists $transactionalLegacyDataBackupCompletedTableSign")
          mySqlJDBCClient.execute(s"drop table if exists $transactionalLegacyDataBackupProgressingTableSign")
          mySqlJDBCClient.execute(s"drop table if exists $transactionalTmpTableSign")
        }
      }
    } catch {
      case e: Exception =>
        throw new MySQLJDBCClientException(s"MySQL Transaction Clean Fail, module: $moduleName, cookie: " + OM.toJOSN(cookies), e)
    }
  }
}
