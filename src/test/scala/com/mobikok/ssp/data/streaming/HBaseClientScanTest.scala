package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
import com.mobikok.ssp.data.streaming.util.OM
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientScanTest {

  val config: Config = ConfigFactory.load

  val conf:SparkConf = null/* new SparkConf()
    .set("hive.exec.dynamic.partition.mode","nonstr4ict")
    .setAppName("wordcount")
    .setMaster("local")*/

  val sc:SparkContext = null//new SparkContext(conf)
  def main (args: Array[String]): Unit = {
    val sc = this.sc
//
    val o = scan0("ssp_user_dwi_uuid_stat_trans_20170620_204927_029__1", null,null, classOf[UuidStat])
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"+OM.toJOSN(o.collect()))
    val o2 = scan0("ssp_user_dwi_uuid_stat_trans_20170620_204927_029__1", null,null, classOf[UuidStat])

    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"+OM.toJOSN(o2.collect()))


    //val a = Array[UuidStat](UuidStat("UUID_TEST", 1).restoreKey(Bytes.toBytes("UUID_TEST")) )
    //puts0(sc, "ssp_user_dwi_uuid_stat_trans_20170620_204927_029__1", a)

  }

  private def puts0 (sc:SparkContext, table: String, hbaseStorables: Array[_ <: HBaseStorable]): Unit = {

    val conf = createWriteConf(table)
    sc.parallelize(hbaseStorables.map { x =>

      val put = new Put(Bytes.toBytes(Bytes.toString(x.toHBaseRowkey)))
      x.toColumns.map { x =>
        put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
      }
      (new ImmutableBytesWritable, put)

    }).saveAsNewAPIHadoopDataset(conf)

  }

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

  private def scan0 (table: String,
                                                                     startRow: Array[Byte],
                                                                     stopRow: Array[Byte],
                                                                     clazz: Class[_ <: HBaseStorable]
                                                                    ): RDD[_ <: HBaseStorable] = {

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
          assemble(x._2, clazz)
        }
    }


    def createReadConf (table: String, scan: Scan): Configuration = {
      val p = ProtobufUtil.toScan(scan);
      val scanStr = Base64.encodeBytes(p.toByteArray());

      @transient val conf = HBaseConfiguration.create()
      config
        .getConfig("hbase.set")
        .entrySet()
        .foreach { x =>
          conf.set(x.getKey, x.getValue.unwrapped().toString)
        }
      // conf.set("hbase.zookeeper.property.clientPort", config.getConfig("hbase.set").getString("hbase.zookeeper.property.clientPort"))
      // conf.set("hbase.zookeeper.quorum", config.getConfig("hbase.set").getString("hbase.zookeeper.quorum"))
      conf.set(TableInputFormat.INPUT_TABLE, table)
      conf.set(TableInputFormat.SCAN, scanStr)
      conf
    }

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
