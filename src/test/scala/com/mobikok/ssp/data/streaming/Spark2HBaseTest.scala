package com.mobikok.ssp.data.streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/6/9.
  */
object Spark2HBaseTest {

  def main (args: Array[String]): Unit = {

//    write()
  read()
  }

  def convert(triple: (Int, String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
    (new ImmutableBytesWritable, p)
  }

  def write():Unit ={
    val sconf = new SparkConf().set("hive.exec.dynamic.partition.mode","nonstrict").setAppName("wordcount").setMaster("local[2]")
    val ssc = new StreamingContext(sconf, Seconds(3*60))
    val sc = ssc.sparkContext

//    //定义 HBase 的配置
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.zookeeper.quorum", "node14,node17,node15")
//    //指定输出格式和输出表名
//    val jobConf = new JobConf(conf,this.getClass)
//    jobConf.setOutputFormat(classOf[TableOutputFormat[ImmutableBytesWritable]])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"spark2hbase_test")
//
//    val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
//    val localData = sc.parallelize(rawData).map(convert)
//
//    localData.saveAsHadoopDataset(jobConf)


      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "node14,node17,node15")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set(TableOutputFormat.OUTPUT_TABLE, "spark2hbase_test_2")
      val job = Job.getInstance(conf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      val init = sc.makeRDD(Array("1,james,32", "2,lebron,30", "3,harden,28"))
      val rdd = init.map(_.split(",")).map(arr => {
        val put = new Put(Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
          (new ImmutableBytesWritable, put)
      })
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def read():Unit = {

    val sconf = new SparkConf().set("hive.exec.dynamic.partition.mode","nonstrict").setAppName("wordcount").setMaster("local")
    val ssc = new StreamingContext(sconf, Seconds(3*60))
    val sc = ssc.sparkContext

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node14,node17,node15")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "spark2hbase_test_2")
    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = usersRDD.count()
    println("spark2hbase_test_2 RDD Count:" + count)
//    usersRDD.cache()
    //遍历输出
    usersRDD.foreach{ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("f".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("f".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }
  }
}
