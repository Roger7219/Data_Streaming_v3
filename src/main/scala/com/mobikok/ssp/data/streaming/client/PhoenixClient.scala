package com.mobikok.ssp.data.streaming.client

import java.util
import java.util.Date
import java.util.regex.Pattern

import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.exception.HBaseClientException
import com.mobikok.ssp.data.streaming.client.cookie.{HBaseTransactionCookie, PhoenixTransactionCookie, TransactionCookie}
import com.mobikok.ssp.data.streaming.util.{Logger, ModuleTracer}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.phoenix.spark._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 过期，用[[com.mobikok.ssp.data.streaming.client.HBaseClient]]代替
  * 暂时只支持单客户端，多可客户端有并发问题
  * Created by Administrator on 2017/6/8.
  */
@deprecated
class PhoenixClient (moduleName:String, sc: SparkContext, config: Config, transactionManager: TransactionManager, moduleTracer:ModuleTracer) extends Transactional {

  val LOG: Logger  = new Logger(moduleName, getClass.getName,new Date().getTime)

  var sqlContext: SQLContext = null
  var hbaseZookeeperQuorum: String = null
  var hbaseClient: HBaseClient = null

  override def init (): Unit = {
    LOG.warn(s"PhoenixClient init started")
    sqlContext = new SQLContext(sc)
//    hbaseClient = new HBaseClient(moduleName +"-inner-hbase-client", sc, config, transactionManager, moduleTracer)
    //hbaseClient.init()
    hbaseZookeeperQuorum = config.getString("hbase.set.hbase.zookeeper.quorum")
    LOG.warn(s"PhoenixClient init completed")
  }

  def gets[T <: HBaseStorable] (table: String,
            constraintPrimarykeys: Array[Seq[Any]],
            hbaseStorableClass: Class[T]
           ): util.ArrayList[T] = {

    val sp = '\u0000'.asInstanceOf[Byte]

    //拼接联合主键
    val rowkeys = constraintPrimarykeys
      .map { x =>
        var row = new mutable.ArrayBuffer[Byte]()

        for(i <- 0 until x.length) {
          val y = x(i)

          if (y.isInstanceOf[Array[Byte]]) {
            row ++=  y.asInstanceOf[Array[Byte]]

          } else if (y.isInstanceOf[String]) {
            row ++= Bytes.toBytes(y.asInstanceOf[String])
            if(i < x.length - 1) {
              row += sp
            }
          } else if (y.isInstanceOf[Int]) {
            row ++= Bytes.toBytes(y.asInstanceOf[Int] - Int.MaxValue - 1)

          } else if (y.isInstanceOf[Long]) {
            row ++=  Bytes.toBytes(y.asInstanceOf[Long] - Long.MaxValue - 1)

          } else {
            throw new HBaseClientException(s"Rowkey(constraint pk) item field type '${x.getClass.getName}' is not supported, value: " + y)
          }
        }
        row.toArray
      }

      hbaseClient.gets(table, rowkeys, hbaseStorableClass)
  }

  def upsertUnionSum (parentTid: String,
                      dmTable: String,
                      hbaseStorableClass: Class[_ <: HBaseStorable],
                      newDF: DataFrame,
                      unionAggExprsAndAlias: List[Column],
                      //sumFields: Array[String],
                      constraintPrimarykeyFields: Array[String],
                      loadTimeField: String = null
                     ): TransactionCookie = {

    val tid = transactionManager.generateTransactionId(parentTid)

    val sp = '\u0000'.asInstanceOf[Byte] //.getBytes //Bytes.toBytes("x") //"\x80".to

    val s = newDF.schema

    //拼接联合主键
    val rowkeys = newDF
      .rdd
      .map { x =>

        var row = new mutable.ArrayBuffer[Byte]()

        for(i <- 0 until constraintPrimarykeyFields.length) {

          val y = constraintPrimarykeyFields(i)

          val t = s.get(s.fieldIndex(y)).dataType
          if(t.isInstanceOf[IntegerType]) {
            row = row ++ Bytes.toBytes(x.getAs[Integer](y) - Int.MaxValue - 1)

          }else if(t.isInstanceOf[LongType]) {
            row = row ++ Bytes.toBytes(x.getAs[Long](y)  - Long.MaxValue - 1)

          }else if(t.isInstanceOf[StringType]) {
            row = row ++ Bytes.toBytes(x.getAs[String](y))
            if(i < constraintPrimarykeyFields.length - 1) {
              row += sp
            }

          }else {
            throw new HBaseClientException(s"Rowkey(constraint pk) item field type '${x.getClass.getName}' is not supported when upsert operating")
          }
        }
        row.toArray

      }.collect()

    LOG.warn(s"PhoenixClient upsertUnionSum innner newDF take(2)", newDF.take(2))

    val original = hbaseClient.getsAsDF(dmTable, rowkeys, hbaseStorableClass /*h.getClass.asInstanceOf[Class[_ <: HBaseStorable]]*/)

    LOG.warn(s"PhoenixClient upsertUnionSum inner gets by rowkeys result take(2)", original.take(2))

    var tail: List[Column] = null
    if(loadTimeField == null) {
      tail = unionAggExprsAndAlias.tail
    } else{
      tail = unionAggExprsAndAlias.tail :+ max(loadTimeField).as(loadTimeField)
    }

    //?
    val updated = newDF
      .union(original)
      .alias("updated")
      .groupBy(constraintPrimarykeyFields.head, constraintPrimarykeyFields.tail: _*)
      .agg(
        unionAggExprsAndAlias.head,
        tail:_*
        /* unionAggExprs.tail :+ /*,*/
          //        sum(sumField),
        max(loadTimeField): _**/
      )

    LOG.warn(s"PhoenixClient upsertUnionSum innner updated DF take(2)", updated.take(2))

    updated.repartition(3).saveToPhoenix(dmTable, zkUrl = Some(hbaseZookeeperQuorum))


    //
    //    val df = sqlContext.load(
    //      "org.apache.phoenix.spark",
    //      Map(
    //        "table" -> dmTable,
    //        "zkUrl" -> hbaseZookeeperQuorum
    //      )
    //    ).where("")

    //
    //    //?
    //    additionalDF//.alias("a")
    //      .union(df)
    //      //.join(combineDF.alias("b"), col("a") === col(""), "left_outer")
    //      .groupBy(groupbyFields.head, groupbyFields.tail:_*)
    //      .agg(
    ////        col("a.times") + col("b.times"),
    //        sum(sumField),
    //        max(loadTimeField)
    //      )
    //
    //    df.saveToPhoenix(dmTable, zkUrl= Some(hbaseZookeeperQuorum))


    //    val df = sqlContext
    //      .read
    //      .format("org.apache.phoenix.spark")
    //      .options(Map("table" -> "TABLE1", "zkUrl" -> hbaseZookeeperQuorum))
    //      .load

    //    df.saveToPhoenix().("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE",
    //    "zkUrl" -> hbaseZookeeperQuorum))

    new PhoenixTransactionCookie(parentTid, tid, null, null, null, null)
  }


  override def commit (cookie: TransactionCookie): Unit = {

  }

  override def rollback (cookies: TransactionCookie*): Cleanable = {
    val cleanable = new Cleanable()
    if (cookies.isEmpty) {
      LOG.warn(s"PhoenixClient rollback started(cookies is empty)")
      //...
      return cleanable
    }
    LOG.warn(s"PhoenixClient rollback started(cookies not empty)")
    //...
    LOG.warn(s"PhoenixClient rollback completed")
    cleanable
  }

  override def clean (cookies: TransactionCookie*): Unit = {

  }
}

