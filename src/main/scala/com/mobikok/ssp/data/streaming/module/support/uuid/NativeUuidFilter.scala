package com.mobikok.ssp.data.streaming.module.support.uuid


import java.util

import breeze.util.BloomFilter
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.util.{BloomFilterWrapper, CSTTime, OM, RunAgainIfError}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * 通过原生的List的方式判断数据有没有重复
  */
class NativeUuidFilter extends UuidFilter {

  @volatile var uuidFilterMap: util.Map[String, util.ArrayList[String]] = new util.HashMap[String, util.ArrayList[String]]()
  var filterBTimeFormat = "yyyy-MM-dd HH:00:00"

  override def dwrNonRepeatedWhere(): String = {
    "repeated = 'N'"
  }

  override def filter(_dwi: DataFrame): DataFrame = {
    val dwi = _dwi.alias("dwi")

    val uuidField = this.dwiUuidFieldsAlias
    LOG.warn(s"uuidF = $uuidField, dwiUuidFieldsAlias = $dwiUuidFieldsAlias")
    val ids = dwi
      .dropDuplicates(dwiUuidFieldsAlias)
      .select(
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time"),
        expr(s"$uuidField")
      ).rdd
      .map { x => (x.getAs[String]("b_time"), x.getAs[String](uuidField)) }
      .groupBy(_._1)
      .collect()

    moduleTracer.trace("    generate uuids")
    LOG.warn("cacheUuidRepeats is null, dwi uuid take(5)", if (ids.nonEmpty) s"${ids.head._1} : ${ids.head._2.take(2)}" else "")

    val b_dates = dwi
      .select(
        to_date(expr(businessTimeExtractBy)).cast("string").as("b_date"),
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), 'yyyy-MM-dd HH:00:00')").as("b_time")
      )
      .dropDuplicates("b_date", "b_time")
      .rdd.map { x => (x.getAs[String]("b_date"), x.getAs[String]("b_time")) }
      .collect()

    loadUuidsIfNonExists(dwiTable, b_dates)
    val saved = filterRepeatedUUID(ids).alias("saved")

    LOG.warn("saved schema fields", saved.schema.fieldNames)
    moduleTracer.trace("    get saved uuid repeats")
    LOG.warn("cacheUuidRepeats is null, Get saved", s"count: ${saved.count()}\ntake(2): ${util.Arrays.deepToString(saved.take(2).asInstanceOf[Array[Object]])}")

    val cacheUUIDRepeats = dwi
      .groupBy(s"dwi.$dwiUuidFieldsAlias")
      .agg(count(lit(1)).alias("repeats"))
      .alias("x")
      .join(saved, col(s"x.$dwiUuidFieldsAlias") === col("saved.uuid"), "left_outer")
      .selectExpr(
        s"x.$dwiUuidFieldsAlias as uuid",
        s"x.repeats + nvl(saved.repeats, 0) as repeats"
      )
      .repartition(shufflePartitions)
      .alias("ur")

    LOG.warn("cacheUuidRepeats info", s"count: ${cacheUUIDRepeats.count()}\nschema fields: ${cacheUUIDRepeats.schema.fieldNames}," +
      s"\ndata take(2): ${util.Arrays.deepToString(cacheUUIDRepeats.take(2).asInstanceOf[Array[Object]])}")


    val newDwi = dwi
      .join(cacheUUIDRepeats, col(s"dwi.$dwiUuidFieldsAlias") === col("ur.uuid"), "left_outer")
      .selectExpr(
        s"nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1)) as repeats",
        s"dwi.*",
        s"if( (nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1))) = 0, 'N', 'Y') as repeated" //,
      )

    newDwi
  }

  def loadUuidsIfNonExists(dwiTable: String, b_dates: Array[(String, String)]): Unit = {
    LOG.warn("BloomFilter try load uuids if not exists start", s"contained b_date: ${OM.toJOSN(uuidFilterMap.keySet())}\ndwi table: $dwiTable\ntry apppend b_dates: ${OM.toJOSN(b_dates)}")

    val bts = neighborBTimes(b_dates.map(_._2))
    LOG.warn("BloomFilter try load history uuids neighborBTimes", bts)

    bts.foreach { case (b_date, b_time) =>

      var filter = uuidFilterMap.get(b_time)
      if (filter == null) {
        LOG.warn("NativeFilter load uuid start", "b_date", b_date, "b_time", b_time)
        //Spark序列化
        val uuidField = dwiUuidFieldsAlias
        var containsData: Array[String] = null
        RunAgainIfError.run({
          containsData = hiveContext
            .read
            .table(dwiTable)
            .where(s"repeated = 'N' and b_date = '$b_date' and from_unixtime(unix_timestamp($businessTimeExtractBy), '$filterBTimeFormat') = '$b_time' ")
            .select(col(uuidField))
            .rdd
            .map { x =>
              val id = x.getAs[String](uuidField)
              if (id != null) id else null
            }
            .collect()
        })
        LOG.warn("read dwi table uuids done", "count ", containsData.length, "b_time ", b_time)

        filter = new util.ArrayList[String](containsData.toList.asJava)
        uuidFilterMap.put(b_time, filter)
      }
    }

    //清除掉不用的，释放内存
    uuidFilterMap = uuidFilterMap.filter { case (b_time, _) =>
      bts.map(_._2).contains(b_time)
    }.asJava

    if (bts.length > 0) {
      moduleTracer.trace("    read dwi table uuids")
    }

    LOG.warn("NativeFilter try load uuids if not exists done")
  }

  def neighborBTimes(b_times: Array[String]): Array[(String, String)] = {
    val result = new util.ArrayList[(String, String)]()

    b_times.foreach { b_time =>
      CSTTime.neighborTimes(b_time, 1.0, 1).foreach { x => result.add((x.split(" ")(0), x)) }
    }
    result.toArray(new Array[(String, String)](0))
  }

  def filterRepeatedUUID(ids: Array[(String, Iterable[(String, String)])]): DataFrame = {

    // 获取重复的rowkey
    val repeatedIds = ids.map { case (_bt, _ids) =>

      val bts = CSTTime.neighborTimes(_bt, 1.0, 1)
//      var initS = 20 * _ids.size // Integer.MAX_VALUE*_ids.size/100000000
      LOG.warn("NativeFilter filter neighborTimes", "currBTime", _bt, "neighborTimes", bts, "sourceBTimes", ids.map(_._1), "dataCount", _ids.size)

      // 存起所有重复的rowkey
      val repeatKeys = _ids.filter { y =>
        val key = y._2
        //        LOG.warn(s"ids._2=$z")
        var result = false
        if (key == null || key.length == 0) {
          result = false
        } else {
          bts.foreach { b_time =>
            if (!result) {
              val filter = uuidFilterMap.get(b_time)

              if (filter.contains(key)) {
                LOG.warn("filterRepeatedUuids：data has repeated", "repeated id: ", key) //test
                result = true
              } else if (_bt.equals(b_time)) {
                filter.add(key)
              }
            }
          }
        }
        result
      }
      //去重后的数据加入当前小时filter中
      var filter = uuidFilterMap.get(_bt)
      if (filter == null) {
        filter = new util.ArrayList[String]()
        uuidFilterMap.put(_bt, filter)
      }
      repeatKeys

    }.flatMap { x => x }.map(_._2)

    val res = hiveContext.createDataFrame(repeatedIds.map { x => UuidStat(x, 1) })
    res
  }
}
