package com.mobikok.ssp.data.streaming.client

import java.net.URLDecoder
import java.sql.ResultSet
import java.util
import java.util.Properties

import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.client.cookie.{MySQLNonTransactionCookie, MySQLRollbackableTransactionCookie, MySQLTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.exception.{MySQLClientException, MySQLJDBCClientException}
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClientV2.Callback
import com.mobikok.ssp.data.streaming.util.{Logger, ModuleTracer, MySqlJDBCClientV2, OM}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

@Deprecated
class MySQLClient(moduleName: String, sc: SparkContext, config: Config, messageClient: MessageClient,
                  transactionManager: TransactionManager, moduleTracer: ModuleTracer) extends Transactional {

  val LOG = new Logger(moduleName, getClass.getName, System.currentTimeMillis())

  var mySqlJDBCClient: MySqlJDBCClientV2 = _
  val OVERWIRTE_FIXED_L_TIME = "0001-01-01 00:00:00"
  private val transactionalTmpTableSign = s"_m_${moduleName}_trans_"
  private val transactionalLegacyDataBackupProgressingTableSign = s"_m_${moduleName}_bp_prog_"
  private val transactionalLegacyDataBackupCompletedTableSign = s"_m_${moduleName}_bp_comp_"

  private val mysqlUser = config.getString("rdb.user")
  private val mysqlPassword = config.getString("rdb.password")

  private val mysqlUrl = config.getString("rdb.url")

  override def init(): Unit = {
    mySqlJDBCClient = new MySqlJDBCClientV2(moduleName, config.getString("rdb.url"), config.getString("rdb.user"), config.getString("rdb.password"))
  }

  def createTableIfNotExist(table: String, like: String): Unit = {
    mySqlJDBCClient.execute(s"create table if not exists $table like $like")
  }

  def overwriteUnionSum(transactionParentId: String,
                        table: String,
                        newDF: DataFrame,
                        selectFields: Array[String],
//                        unionAggExprsAndAlias: List[Column]
                        aggExprsAlias: List[String]
                       ): MySQLTransactionCookie = {
    val ts = Array("l_time", "b_date", "b_time")
    //    val ts = Array(hivePartitionField) ++ hivePartitionFields

    val ps = newDF
      .dropDuplicates(ts)
      .collect()
      .map { x =>
        ts.map { y =>
          HivePartitionPart(y, x.getAs[String](y))
        }
      }
    moduleTracer.trace("    get update partitions")

    //    overwriteUnionSum(transactionParentId, table, newDF, unionAggExprsAndAlias, groupByFields, ps, ts.head, ts.tail:_*)
    overwriteUnionSum(transactionParentId, table, newDF, selectFields, aggExprsAlias, ps, ts.head, ts.tail: _*)
  }

  def overwriteUnionSum(transactionParentId: String,
                        table: String,
                        newDF: DataFrame,
                        selectFields: Array[String],
//                        unionAggExprsAndAlias: List[Column],
                        aggExprsAlias: List[String],
                        ps: Array[Array[HivePartitionPart]],
                        hivePartitionField: String,
                        hivePartitionFields: String*): MySQLTransactionCookie = {
    var tid: String = null
    LOG.warn(s"MySQLClient overwriteUnionSum start")
    try {
//      LOG.warn(s"select fields: \n${selectFields.mkString(",")}")
      // 自动识别group by字段和agg字段
      val groupByFields = autoRecognizeGroupByFields(selectFields)
//      LOG.warn(s"""group by fields: \n${groupByFields.mkString(",")}""")
//      val aggFields = autoRecognizeAggFields(selectFields)
//      LOG.warn(s"""agg fields: \n${aggFields.mkString(",")}""")
//      val aggColumns: Array[Column] = aggFields.map { x => expr(s"sum($x) as $x") }

      tid = transactionManager.generateTransactionId(transactionParentId)
      var tt = table + transactionalTmpTableSign + tid
      var pt = table + transactionalLegacyDataBackupProgressingTableSign + tid
      var ct = table + transactionalLegacyDataBackupCompletedTableSign + tid

      val ts = Array(hivePartitionField) ++ hivePartitionFields

      val where = partitionsWhereSQL(ps)
      LOG.warn(s"MySQLClient overwriteUnionSum overwrite partitions where", where)

      //      val ts = Array("l_time", "b_date", "b_time")

      //      val fields = config.getConfigList(s"modules.$moduleName.dwr.mysql.fields").toList


      //      val original = sqlContext.read.format("jdbc")
      //        .option("driver", "com.mysql.jdbc.Driver")
      //        .option("url", config.getString("rdb.url"))
      //        .option("user", config.getString("rdb.user"))
      //        .option("password", config.getString("rdb.password"))
      //        .option("dbtable", table)
      //        .load()
      //      val newSchema = newDF.schema.fields.map{ x => x.name }
      val baseTableExists = mySqlJDBCClient.executeQuery(
        s"""show tables like "$table"""", new Callback[Boolean] {
          override def onCallback(rs: ResultSet): Boolean = {
            rs.first()
          }
        })


      var updated: DataFrame = null
      if (baseTableExists) {
//        LOG.warn("select sql", s"select * from $table where $where")
        val original: DataFrame = readFromMySQL(s"select * from $table where $where")
//        original.cache()
        val fs: Array[String] = original.schema.fieldNames
        LOG.warn(s"""original data schema, ${fs.mkString(", ")}""")

        val gs = (groupByFields :+ hivePartitionField) ++ hivePartitionFields
        LOG.warn(s"""group by:\n ${gs.mkString(", ")}""")
        updated = original.union(newDF)
          .groupBy(gs.head, gs.tail: _*)
          .agg(
            aggExprsAlias.map{x => sum(x).as(x)}.head,
            aggExprsAlias.map{x => sum(x).as(x)}.tail:_*
          )
          .select(fs.head, fs.tail: _*)
      } else {
        updated = newDF
      }

      updated.cache()
      LOG.warn(s"updated data count: ${updated.count()}")

      if (transactionManager.needTransactionalAction()) {
        if (baseTableExists) {
          LOG.warn(s"MySQLClient", s"base table exists, create table $tt")
//          createTableIfNotExist(tt, table)
          appendToMySQL(tt, updated)
        } else {
          LOG.warn(s"MySQLClient", s"base table not exists, overwrite table $tt")
          overwriteToMySQL(tt, updated)
          createTableIfNotExist(table, tt)
        }
      } else {
        mySqlJDBCClient.execute(s"delete from $table where $where")
        appendToMySQL(table, updated)
        updated.unpersist()
        return new MySQLNonTransactionCookie(transactionParentId, tid, table)
      }

      val isEmpty = newDF.take(1).isEmpty
      moduleTracer.trace("     mysql union sum and write")

      updated.unpersist()
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
    if ("".equals(w)) w = "'no_partition_specified' <> 'no_partition_specified'"
    "( " + w + " )"
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

        var bw = c.partitions.map { x => x.filter(y => "l_time".equals(y.name) && !y.value.equals(OVERWIRTE_FIXED_L_TIME)) }

        val flatBw = bw.flatMap { x => x }.toSet
        LOG.warn(s"flatBw: \n$flatBw")
        if (bw.length > 1) {
          throw new MySQLClientException(s"l_time must be only one value for backup, But has ${flatBw.size} values: $flatBw !")
        }

        var where = partitionsWhereSQL(c.partitions)

        LOG.warn(s"MySQL before commit backup table ${c.targetTable} where: ", bw)

        //        mySqlJDBCClient.execute(s"INSERT INTO $transactionalLegacyDataBackupProgressingTableSign SELECT * FROM ${c.targetTable} WHERE $where")
        // 保证事务
        val originalData = readFromMySQL(s"select * from ${c.targetTable} where $where")
        overwriteToMySQL(c.transactionalProgressingBackupTable, originalData)

        moduleTracer.trace("    insert into mysql progressing backup table")
        mySqlJDBCClient.execute(s"alter table ${c.transactionalProgressingBackupTable} rename to ${c.transactionalCompletedBackupTable}")
        moduleTracer.trace("    rename to mysql completed backup table")


        //        val tmpData = readFromMySQL(s"select * from ${c.transactionalTmpTable} where $where")
        //        val updatedDataSqls = tmpData.collect().map{ x => s"insert into ${c.targetTable} values(${x.mkString(", ")})"}
        //        mySqlJDBCClient.executeBatch(updatedDataSqls)
        // update
        val updateSqls = Array(
          s"delete from ${c.targetTable} where $where",
          s"insert into ${c.targetTable} select * from ${c.transactionalTmpTable}")
        mySqlJDBCClient.executeBatch(updateSqls)
        moduleTracer.trace("    insert into mysql target table")
      }

      //      tryAsyncCompaction(c)
    }
  }

  val sqlContext = new SQLContext(sc)

  // 懒加载
  private def readFromMySQL(selectSQL: String): DataFrame = {
    sqlContext.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
//      .option("url", config.getString("rdb.url"))
//      .option("user", config.getString("rdb.user"))
//      .option("password", config.getString("rdb.password"))
      .option("url", mysqlUrl)
      .option("user", mysqlUser)
      .option("password", mysqlPassword)
      .option("dbtable", s"($selectSQL) as t1")
      .load()
  }

  val properties = new Properties()
  properties.setProperty("driver", "com.mysql.jdbc.Driver")
  properties.setProperty("user", mysqlUser)
  properties.setProperty("password", mysqlPassword)

  // 直接覆盖所有表的所有内容
  private def overwriteToMySQL(tableName: String, data: DataFrame): Unit = {
    // drop -> create -> insert
//    LOG.warn("overwrite to mysql", s"overwrite table $tableName, data take(1): ${data.take(1).mkString(",")}")
//    LOG.warn("overwrite to mysql",s"url=\n${config.getString("rdb.url")}\ntable name:\n$tableName")
    data.write.mode(SaveMode.Overwrite).jdbc(config.getString("rdb.url"), tableName, properties)
  }

  private def appendToMySQL(tableName: String, data: DataFrame): Unit = {
    data.write.mode(SaveMode.Append).jdbc(config.getString("rdb.url"), tableName, properties)
  }

  private def updateToMySQL(tableName: String, where: String, data: DataFrame): Unit = {

  }

  private def partitionsAlterSQL(ps: Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.map { y =>
        y.name + "=\"" + y.value + "\""
        //y._1 + "=" + y._2
      }.mkString(", ")
    }
  }

  override def rollback(cookies: TransactionCookie*): Cleanable = {
    try {
      val cleanable = new Cleanable
      if (cookies.isEmpty) {
        LOG.warn(s"HiveClient rollback started(cookies is empty)")

        val completeTables = mySqlJDBCClient.executeQuery(s"show tables like '%$transactionalLegacyDataBackupCompletedTableSign%'", new Callback[Array[String]] {
          override def onCallback(rs: ResultSet): Array[String] = {
            val tables = new util.ArrayList[String]()
            while (rs.next()) {
              tables.add(rs.getString(1))
            }
            tables.asScala.toArray
          }
        })

        LOG.warn("MySQLClient rollback check backup completed table", completeTables)

        completeTables.sortBy { x =>
          (
            x.split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(transactionalLegacyDataBackupProgressingTableSign)(0),
            Integer.parseInt(
              x.split(transactionalLegacyDataBackupCompletedTableSign)(1)
                .split(TransactionManager.parentTransactionIdSeparator)(1))
          )
        }
          .reverse
          .foreach { completeTable =>
            val parentTid = completeTable
              .split(transactionalLegacyDataBackupCompletedTableSign)(1)
              .split(TransactionManager.parentTransactionIdSeparator)(0)

            val tid = completeTable.split(transactionalLegacyDataBackupCompletedTableSign)(1)

            val needRollback = transactionManager.isActiveButNotAllCommited(parentTid)
            if (needRollback) {
              val batchCount = mySqlJDBCClient.executeQuery(s"select count(1) from $completeTable", new Callback[Integer] {
                override def onCallback(rs: ResultSet): Integer = {
                  rs.next()
                  rs.getInt(1)
                }
              })

              val targetTable = completeTable.split(transactionalLegacyDataBackupCompletedTableSign)(0)

              val ts = Array(completeTable)
              val ps = partitions(ts: _*)

              lTimePartitionsAlterSQL(ps).distinct.foreach { where =>
                // drop table if exists partiton y
                mySqlJDBCClient.execute(s"delete from $targetTable where $where")
              }

              if (batchCount == 0) {
                LOG.warn(s"MySQL rollback", s"Reverting to the legacy data, Backup table is empty, delete table of targetTable!! \ntable: $targetTable \nwhere: ${partitionsWhereSQL(ps)}")
              } else {
                mySqlJDBCClient.execute(s"insert into $targetTable select * from $completeTable")
                //                val data = readFromMySQL(s"select * from $completeTable")
                //                appendToMySQL(targetTable, data)
                //                val sqls = data.collect().map{ x => s"""insert into $x values (${x.mkString(", ")})"""}
                //                mySqlJDBCClient.executeBatch(sqls)
                //                LOG.warn(s"MySQL rollback", s"""insert data sqls:\n${sqls.mkString(",\n")}""")
              }
            }
          }

        return cleanable
      }

      cleanable
    } catch {
      case e: Exception =>
        throw new MySQLJDBCClientException(s"Hive Transaction Commit Fail, module: $moduleName,  cookie: " + cookies, e)
    }
  }

  def partitions(tableNames: String*): Array[Array[HivePartitionPart]] = {
    val ts = tableNames
    //tableName +: tableNames
    val ls = mutable.ListBuffer[Array[HivePartitionPart]]()
    ts.foreach { y =>
      val tt = y
      //      sql(s"show partitions $tt")
      //        .collect()
      //        .foreach{z=>
      //          ls += parseShowPartition(z.getAs[String]("partition"))
      //        }

    }
    ls.toArray[Array[HivePartitionPart]]
  }

  def parseShowPartition(partition: String): Array[HivePartitionPart] = {
    partition
      .split(s"/")
      .map { x =>
        val s = x.split("=")
        HivePartitionPart(URLDecoder.decode(s(0), "utf-8"), URLDecoder.decode(s(1), "utf-8"))
      }
  }

  private def lTimePartitionsAlterSQL(ps: Array[Array[HivePartitionPart]]): Array[String] = {
    ps.map { x =>
      x.filter { z => "l_time".equals(z.name) }
        .map { y =>
          y.name + "=\"" + y.value + "\""
          //y._1 + "=" + y._2
        }.mkString(", ")
    }
  }

  override def clean(cookies: TransactionCookie*): Unit = {
    try {
      cookies.foreach {
        case cookie: MySQLRollbackableTransactionCookie =>
          mySqlJDBCClient.execute(s"drop table if exists ${cookie.transactionalCompletedBackupTable}")
          mySqlJDBCClient.execute(s"drop table if exists ${cookie.transactionalProgressingBackupTable}")
          mySqlJDBCClient.execute(s"drop table if exists ${cookie.transactionalTmpTable}")
        //          mySqlJDBCClient.execute(s"drop table if exists $transactionalLegacyDataBackupCompletedTableSign")
        //          mySqlJDBCClient.execute(s"drop table if exists $transactionalLegacyDataBackupProgressingTableSign")
        //          mySqlJDBCClient.execute(s"drop table if exists $transactionalTmpTableSign")
        case _ =>
      }
    } catch {
      case e: Exception =>
        throw new MySQLJDBCClientException(s"MySQL Transaction Clean Fail, module: $moduleName, cookie: " + OM.toJOSN(cookies), e)
    }
  }

  def autoRecognizeGroupByFields(selectFields: Array[String]): Array[String] = {
    val allGroupByFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.fields").map { x => x.getString("as") }
    selectFields.filter { x => allGroupByFields.contains(x) }
  }

  def autoRecognizeAggFields(selectFields: Array[String]): Array[String] = {
    val allAggFields = config.getConfigList(s"modules.$moduleName.dwr.groupby.aggs").map {
      x => x.getString("as")
    }.toList
    selectFields.filter { x => allAggFields.contains(x) }
  }
}
