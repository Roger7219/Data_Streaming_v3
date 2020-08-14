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
  * 建议改用BTimeRangeUuidFilter
  * Created by Administrator on 2018/4/17.
  */
class DefaultRepeatsFilter(dwiBTimeFormat: String = "yyyy-MM-dd HH:00:00") extends RepeatsFilter{

  @volatile var uuidBloomFilterMap:util.Map[String, BloomFilterWrapper] = new util.HashMap[String, BloomFilterWrapper]()
  var bloomFilteBTimeformat: String = dwiBTimeFormat //_ //"yyyy-MM-dd HH:00:00"

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
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilteBTimeformat}')").as("b_time")
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

    val b_dates = dwi
      .select(
        to_date(expr(businessTimeExtractBy)).cast("string").as("b_date"),
        expr(s"from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilteBTimeformat}')").as("b_time")
      )
      .dropDuplicates("b_date", "b_time")
      .rdd.map{
      x=> (
        x.getAs[String]("b_date"),
        x.getAs[String]("b_time")
      )
    }.collect()

    loadUuidsIfNonExists(dwiTable, b_dates)
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
    var result = new util.ArrayList[(String, String)]()

    b_times.foreach{bt=>
      // CSTTime.neighborTimes(bt, 1.0, 1).foreach{x=> result.add((x.split(" ")(0), x))}
      // for nadx
      CSTTime.neighborBTimes(bt, 0).foreach{ x=> result.add((x.split(" ")(0), x))}
    }
    result.toArray(new Array[(String, String)](0))
  }


  def loadUuidsIfNonExists(dwiTable: String, b_dates:Array[(String, String)]): Unit = {
    LOG.warn("BloomFilter try load uuids if not exists start", s"contained b_date: ${OM.toJOSN(uuidBloomFilterMap.keySet())}\ndwi table: $dwiTable\ntry apppend b_dates: ${OM.toJOSN(b_dates)}")

    val bts = neighborBTimes(b_dates.map(_._2).filter{x=>StringUtil.notEmpty(x)})
    LOG.warn("BloomFilter try load history uuids neighborBTimes", bts)

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
            .where(s"repeated = 'N' and b_date = '${b_date}' and from_unixtime(unix_timestamp($businessTimeExtractBy), '${bloomFilteBTimeformat}') = '${b_time}' ")
            .select(col(a))
            .rdd
            .map{x=>
              var id = x.getAs[String](a)
              if(id != null) id.getBytes() else null
            }
            .collect()
        })
        LOG.warn("read dwi table uuids done", "count ", c.length, "b_time ", b_time)

//        val bf = new BloomFilter(math.max(20,/*(Int.MaxValue/100000000)*/ 20 * c.length), 12 /*16*/, Hash.MURMUR_HASH)

        val vectorSize = if(c.length == 0) 40 else c.length * 40
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
    val repeatedIds = ids.map{ case(_bt, _ids) =>

      // var bts = CSTTime.neighborTimes(_bt, 1.0, 1)
      // for nadx
      var bts = CSTTime.neighborBTimes(_bt, 0)

      val vectorSize = if(_ids.size == 0) 40 else _ids.size * 40
      // Integer.MAX_VALUE*_ids.size/100000000
      LOG.warn("BloomFilter filter neighborTimes", "currBTime", _bt, "neighborTimes", bts, "sourceBTimes", ids.map(_._1), "dataCount", _ids.size, "vectorSize", vectorSize)

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
              }else if(_bt.equals(bt)){
                bf.add(k)
              }
            }
          }
        }
        re
      }
      //去重后的数据加入当前小时bf中
      var wrap = uuidBloomFilterMap.get(_bt)
      if(wrap == null) {
        wrap = new BloomFilterWrapper()
        uuidBloomFilterMap.put(_bt, wrap)
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
