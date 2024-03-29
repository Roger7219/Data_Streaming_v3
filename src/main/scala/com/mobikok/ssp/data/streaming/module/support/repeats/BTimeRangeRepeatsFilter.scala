package com.mobikok.ssp.data.streaming.module.support.repeats

import java.util

import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.util._
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * 去重功能
  *
  * 比如 new BTimeRangeRepeatsFilter("yyyy-MM-dd HH:00:00", List(-1,1))
  * 其中"yyyy-MM-dd HH:00:00"表示dwi的b_time是精确到了小时，
  * List(-1,1)表示当前批次处理中，取dwi的前一个小时、当前小时和后一个小时，这3小时内的数据来判断是否重复
  *
  * 指定开始和结束的时间区间，通过startOffsetHour和endOffsetHour
  * Created by Administrator on 2018/4/17.
  */
class BTimeRangeRepeatsFilter(dwiBTimeFormat: String = "yyyy-MM-dd HH:00:00", bTimeRange: List[Int]) extends RepeatsFilter{

  @volatile var uuidBloomFilterMap:util.Map[String, BloomFilterWrapper] = new util.HashMap[String, BloomFilterWrapper]()
  var bloomFilterBTimeFormat: String = dwiBTimeFormat

  // 这个值越大过滤精度越高(即本身不是重复的，却被误当重复的过滤掉的概率越低)，但内存占用越高，
  // 40 来自于2倍的Int.MaxValue/100000000，设置为40是占用内存和过滤精度的平衡点，是目前采用的
  val wronglyFilterIndex = 40

  val bTimeStartOffsetHour = bTimeRange(0)
  val bTimeEndOffsetHour = bTimeRange(1)

  def this(dwiBTimeFormat: String, bTimeSameStartAndEndOffsetHourRange: Int){
    this(dwiBTimeFormat, List( - bTimeSameStartAndEndOffsetHourRange, bTimeSameStartAndEndOffsetHourRange))
  }

  override def dwrNonRepeatedWhere (): String = {
    "repeated = 'N'"
  }

  override def filter (_dwi: DataFrame): DataFrame = {
    val dwi = _dwi.alias("dwi")

    var _uuidF = this.dwiUuidFieldsAlias
    LOG.warn(s"uuidF = ${_uuidF}, dwiUuidFieldsAlias = $dwiUuidFieldsAlias")
    val ids = dwi
      .dropDuplicates(dwiUuidFieldsAlias)
      .select(
        expr(s"$dwiUuidFieldsAlias"),
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilterBTimeFormat}')").as("b_time")
      )
      .rdd
      .map { x =>
        (x.getAs[String]("b_time"), x.getAs[String](_uuidF) )
      }
      .filter{x=>StringUtil.notEmpty(x._1)}
      .groupBy(_._1)
      .collect()

    moduleTracer.trace("    generate uuids")
    LOG.warn("cacheUuidRepeats is null, dwi uuid take(5)", if(ids.nonEmpty) s"${ids.head._1} : ${ids.head._2.take(2)}" else "")

    //            var ids = dwi
    //              .select(
    //                to_date(expr(businessDateExtractBy)).as("b_date"),
    //                col(dwiUuidFieldsAlias)
    //              )
    //              .dropDuplicates("b_date", dwiUuidFieldsAlias)
    //              .rdd
    //              .collect()
    //            filterRepeatedUuids(ids)

    //            saved = repeatedCount(dwi).alias("saved")

    val b_times = dwi
      .select(
        to_date(expr(businessTimeExtractBy)).cast("string").as("b_date"),
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilterBTimeFormat}')").as("b_time")
      )
      .dropDuplicates("b_date", "b_time")
      .rdd.map{
      x=> (
        x.getAs[String]("b_date"),
        x.getAs[String]("b_time")
      )
    }.collect()

    loadUuidsIfNonExists(dwiTable, b_times)
    val saved = filterRepeatedUuids(ids).alias("saved")

    LOG.warn("saved schema fields", saved.schema.fieldNames)
    //
    ////            val saved = hbaseClient.getsAsDF(dwiUuidStatHbaseTable, ids, classOf[UuidStat]).alias("saved")

    moduleTracer.trace("    get saved uuid repeats")
    LOG.warn("cacheUuidRepeats is null, Get saved", s"count: ${saved.count()}\ntake(2): ${util.Arrays.deepToString(saved.take(2).asInstanceOf[Array[Object]])}" )

    var cacheUuidRepeats = dwi
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

    LOG.warn("cacheUuidRepeats info", s"count: ${cacheUuidRepeats.count()}\nschema fields: ${cacheUuidRepeats.schema.fieldNames}," +
      s"\ndata take(2): ${util.Arrays.deepToString(cacheUuidRepeats.take(2).asInstanceOf[Array[Object]])}" )

    // ???           cacheUuidRepeats = hiveContext.createDataFrame(cacheUuidRepeats.collectAsList(), cacheUuidRepeats.schema).repartition(shufflePartitions).alias("ur")
//    cacheUuidRepeats.persist(StorageLevel.MEMORY_ONLY_SER)
//    cacheUuidRepeats.count()

//    LOG.warn("cacheUuidRepeats is null, cacheUuidRepeats", s"count: ${cacheUuidRepeats.count()}\ntake(2): ${util.Arrays.deepToString(cacheUuidRepeats.take(2).asInstanceOf[Array[Object]])}")

    //cacheUuidRepeats.collect()

    var newDwi = dwi
      .join(cacheUuidRepeats, col(s"dwi.$dwiUuidFieldsAlias") === col("ur.uuid"), "left_outer")
      .selectExpr(
        s"nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1)) as repeats",
        s"dwi.*",
        s"if( (nvl(ur.repeats, 0) - (row_number() over(partition by dwi.$dwiUuidFieldsAlias order by 1))) = 0, 'N', 'Y') as repeated"//,
//        s"$dwiLTimeExpr as l_time"
      )

//    cacheUuidRepeats.unpersist()

    newDwi
    //cacheUuidRepeats
  }

  def neighborBTimes(b_times: Array[String]): Array[(String, String)] ={
    var result = new util.HashSet[(String, String)]()
    b_times.foreach{bt=>
      CSTTime.neighborBTimes(bt, bTimeStartOffsetHour, bTimeEndOffsetHour).foreach{ x=> result.add((x.split(" ")(0), x))}
    }
    result.toArray(new Array[(String, String)](0))
  }

  def loadUuidsIfNonExists(dwiTable: String, b_times:Array[(String, String)]): Unit = {
    LOG.warn("BloomFilter try load uuids if not exists start", s"contained b_date: ${OM.toJOSN(uuidBloomFilterMap.keySet())}\ndwi table: $dwiTable\ntry apppend b_dates: ${OM.toJOSN(b_times)}")

    val bts = neighborBTimes(b_times.map(_._2).filter{ x=>StringUtil.notEmpty(x)})
    LOG.warn("BloomFilter try load history uuids neighborBTimes", "bTimes", bts, "bTimeStartOffsetHour", bTimeStartOffsetHour, "bTimeEndOffsetHour", bTimeEndOffsetHour)

    bts.foreach{case(b_date, b_time)=>

      var f = uuidBloomFilterMap.get(b_time)
      if(f == null) {
        LOG.warn("BloomFilter load uuids start", "b_date", b_date, "b_time", b_time)

        //        f = new BloomFilter(Integer.MAX_VALUE, 16, Hash.MURMUR_HASH);
        //        uuidBloomFilterMap.put(b_time, f)

        //Spark序列化
        val a = dwiUuidFieldsAlias
        var c: Array[Array[Byte]] = null
        RunAgainIfError.run({
          c = hiveContext
            .read
            .table(dwiTable)
//            .where(s"repeated = 'N' and from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilterBTimeFormat}') = '${b_time}' ")
            .where(s"repeated = 'N' and b_time = '${b_time}' ")
            .select(col(a))
            .rdd
            .map{x=>
              var id = x.getAs[String](a)
              if(id != null) id.getBytes() else null
            }
            .collect()
        })
        LOG.warn("read dwi table uuids done", "count ", c.length, "b_time ", b_time)

        val vectorSize = math.max(wronglyFilterIndex*c.length, wronglyFilterIndex) * bts.length
        val bf = new BloomFilter(vectorSize, 16, Hash.MURMUR_HASH)
        var wrap = uuidBloomFilterMap.get(b_time)
        if(wrap == null) {
          wrap = new BloomFilterWrapper()
          uuidBloomFilterMap.put(b_time, wrap)
        }
        wrap.append(bf)

        c.foreach{x=>
          if(x != null && x.length > 0) {
            bf.add(new Key(x))
          }
        }

        //          c.take(1).foreach{x=>
        //            LOG.warn("BloomFilter test uuids filter by", x)
        //            LOG.warn("BloomFilter test uuids filter result", f.membershipTest(new Key(x)))
        //          }
      }

    }

    //清除掉不用的，释放内存
    uuidBloomFilterMap = uuidBloomFilterMap.filter{case(bt, _)=>
      bts.map(_._2).contains(bt)
    }.asJava

    if(bts.size > 0) {
      moduleTracer.trace("    read dwi table uuids")
    }

    LOG.warn("BloomFilter try load uuids if not exists done")
  }

  def filterRepeatedUuids(ids: Array[(String, Iterable[(String, String)])] /*Array[String]*/): DataFrame ={

    //Uuid BloomFilter 去重（重复的rowkey）
    val repeatedIds = ids.map{ case(_btime, _ids) =>

      var bts = CSTTime.neighborBTimes(_btime, bTimeStartOffsetHour, bTimeEndOffsetHour)

      val vectorSize = math.max(wronglyFilterIndex*_ids.size, wronglyFilterIndex) * bts.length
      LOG.warn("BloomFilter filter neighborTimes", "currBTimes", _btime, "neighborTimes", bts, "sourceBTimes", ids.map(_._1), "dataCount", _ids.size, "vectorSize", vectorSize)

      val bf = new BloomFilter(vectorSize, 16, Hash.MURMUR_HASH)

      // 存起所有重复的rowkey
      val reps = _ids.filter{ y=>
        val z = y._2
//        LOG.warn(s"ids._2=$z")
        var re = false
        if(z == null || z.length == 0){
          re = false
        }else {

          bts.foreach{bt=>
            if(!re) {
              val f = uuidBloomFilterMap.get(bt)

//              LOG.warn("filterRepeatedUuids", "uuidBloomFilterMap", uuidBloomFilterMap)
              val k = new Key(z.getBytes())

              if(f.membershipTest(k)) {
//                LOG.warn("filterRepeatedUuids：data has repeated" , "repeated id: ",z)//test
                re = true
              }else if(_btime.equals(bt)){
                bf.add(k)
              }
            }
          }
        }
        re
      }
      //去重后的数据加入当前小时bf中
      var wrap = uuidBloomFilterMap.get(_btime)
      if(wrap == null) {
        wrap = new BloomFilterWrapper()
        uuidBloomFilterMap.put(_btime, wrap)
      }
      wrap.append(bf)

      reps

    }.flatMap{x=>x}.map(_._2)

    //拓展,对当前批次生成对应的BF(前批次去重后的数据)
    /*ids.foreach{case(bt, _ids)=>
      LOG.warn("filterRepeatedUuids：BloomFilter" , "take 4 ids", _ids.map(x=>x._2).take(4).mkString("[", ",", "]"), "add id count: ", _ids.count(x=> x._2 != null))//test
      val deRepeateds = _ids.filter()

      val bf = new BloomFilter(math.max(20,/*(Int.MaxValue/100000000)*/ 20 * _ids.count(x=> x._2 != null)), 12 /*16*/, Hash.MURMUR_HASH)

      _ids.foreach{x=>
        bf.add(new Key(x._2.getBytes))
      }
      var wrap = uuidBloomFilterMap.get(bt)
      if(wrap == null) {
        wrap = new BloomFilterWrapper()
        uuidBloomFilterMap.put(bt, wrap)
      }
      wrap.append(bf)
    }*/

    val res = hiveContext.createDataFrame(repeatedIds.map{x=> new UuidStat(x, 1)})
    res
  }
}
