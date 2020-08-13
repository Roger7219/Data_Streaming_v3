package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.HBaseClientPutTest.sparkConf
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.client.HBaseClient
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import scala.collection.mutable.Map

/**
  * Created by Administrator on 2017/6/21.
  */
class HBaseScan{

  def scan0 (table: String,
             startRow: Array[Byte],
             stopRow: Array[Byte],
             clazz: Class[_ <: HBaseStorable]
            ):  RDD[_ <: HBaseStorable] = {


    val transactionalLegacyDataBackupCompletedTableSign = "_backup_completed_"
    val transactionalLegacyDataBackupProgressingTableSign = "_backup_progressing_"

    val backupCompletedTableNameWithinClassNamePattern = "_class_(.+)".r

    val transactionalTmpTableSign = "_trans_"

    val scan = new Scan()
    if (startRow != null) scan.setStartRow(startRow)
    if (stopRow != null) scan.setStopRow(stopRow)

    val conf = createReadConf(table, scan)

    var sc = HBaseClientTest.sc
    val rdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //HBaseStorable
    //    rdd.map { case (b, result) =>
    //      assemble(result, clazz)
    //    }

    var _clazz = clazz

    rdd
      .map { x =>
        HBaseScan.assemble(x._2, _clazz)
      }
      .filter{x=> x != null}

//    rdd
//      .map { x =>
//        assemble(x._2, _clazz)
//      }
//      .filter{x=> x != null}
  }

  def createReadConf (table: String, scan: Scan): Configuration = {
    val p = ProtobufUtil.toScan(scan);
    val scanStr = Base64.encodeBytes(p.toByteArray());

    val conf = HBaseConfiguration.create()
    HBaseClientTest.config
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


}
object HBaseScan{
  private def assemble (result: Result, clazz: Class[_ <: HBaseStorable]): HBaseStorable = {
    if(result == null || result.isEmpty) {
      return null
    }
    val h = clazz.newInstance()
    h//.setRowkey(result.getRow)

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
