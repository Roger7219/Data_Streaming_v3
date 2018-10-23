package com.mobikok.ssp.data.streaming

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/6/9.
  */
object ALSModelLoadTest {

  def main (args: Array[String]): Unit = {

//    classOf[MatrixFactorizationModel]
//    val sc:SparkContext = new SparkContext("local[2]", "sc")
//
//    val model:MatrixFactorizationModel  = MatrixFactorizationModel.load(sc,
//      "/root/kairenlo/data-streaming/dw_rating/zz_model_output_dir/20170731_134856_143.model");
//
//    val ob=java.util.Arrays.deepToString(model.recommendProducts(1885878, 10).asInstanceOf[Array[Object]])
//
//    println("===="+ob)
//    println(aHivePartitionRecommendedFileNumber("", 8, 11, 11))
    println(Math.ceil(8D/11))

  }

  def aHivePartitionRecommendedFileNumber(logTip: String, shufflePartitions: Int, rddPartitions: Int, hivePartitions: Int): Int ={
    var result = 0
    if(rddPartitions == 0 || hivePartitions == 0) {
      result = 1
    }else {
      result = Math.ceil(shufflePartitions/hivePartitions).toInt
    }

    result
  }
}
