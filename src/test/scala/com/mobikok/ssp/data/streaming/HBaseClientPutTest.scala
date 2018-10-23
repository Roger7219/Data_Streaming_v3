package com.mobikok.ssp.data.streaming

import com.mobikok.ssp.data.streaming.HBaseClientScanTest.{config, scan0}
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.entity.feature.HBaseStorable
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

/**
  * Created by Administrator on 2017/6/20.
  */
object HBaseClientPutTest  {

  var config: Config = ConfigFactory.load

  var sparkConf:SparkConf =  null /*new SparkConf()
    .set("hive.exec.dynamic.partition.mode", "nonstr4ict")
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .setAppName("wordcount")
    .setMaster("local")
*/
  val sc:SparkContext = null//new SparkContext(sparkConf)

  def main (args: Array[String]): Unit = {




    val a = Array[UuidStat](UuidStat("UUID_TEST3", 1))//.setRowkey(Bytes.toBytes("UUID_TEST3")))

    try {


      //    puts0(sc, "ssp_user_dwi_uuid_stat_trans_20170620_204927_029__1", a)


      val C = createWriteConf()

          val b=a.map { x =>

            val put = new Put(Bytes.toBytes(Bytes.toString(x.toHBaseRowkey)))
            x.toColumns.map { x =>
              put.addColumn(Bytes.toBytes(x._1._1), Bytes.toBytes(x._1._2), x._2)
            }
            (new ImmutableBytesWritable, put)

          }

      //val b2: Array[(ImmutableBytesWritable, Put)] = Array[(ImmutableBytesWritable, Put)](
       // (new ImmutableBytesWritable, new Put(Bytes.toBytes("asd") )) )



      var rdd = sc.parallelize(b)

      //
  //    val indataRDD = sc.makeRDD(Array("12,jack,15", "2,Lily,16", "3,mike,16"))
  //
  //    rdd = indataRDD.map(_.split(',')).map { arr => {
  //      /*一个Put对象就是一行记录，在构造方法中指定主键
  //       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
  //       * Put.add方法接收三个参数：列族，列名，数据
  //       */
  //      val put = new Put(Bytes.toBytes(arr(0).toInt))
  //      put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
  //      put.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
  //      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
  //      (new ImmutableBytesWritable, put)
  //    }}

      rdd.saveAsNewAPIHadoopDataset(C)

      val o = scan0("ssp_user_dwi_uuid_stat_trans_20170621_003748_367__1", null,null, classOf[UuidStat])
      o.collect()
      .map {
        { x =>
          println("======" + x)
        }
      }

    }catch {
      case(ex: Exception) =>  ex.printStackTrace()
    }

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



  private def createWriteConf (): Configuration = {
    val conf = HBaseConfiguration.create

    conf.set("hbase.zookeeper.quorum", "node14,node17,node15")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "ssp_user_dwi_uuid_stat_trans_20170621_003748_367__1")
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[org.apache.hadoop.hbase.io.ImmutableBytesWritable]])
    job.getConfiguration
  }


}
