package com.mobikok.ssp.data.streaming

import java.util

import com.mobikok.ssp.data.streaming.AS3.ll
import com.mobikok.ssp.data.streaming.entity.UuidStat
import com.mobikok.ssp.data.streaming.util.OM
import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.reflect.macros.whitebox

/**
  * Created by Administrator on 2017/12/11.
  */
class AS3(sqlC: SQLContext){
  import org.apache.spark.broadcast.Broadcast
  import scala.collection.JavaConversions._

  def m(): Unit ={

    val broadcast = sqlC.sparkContext.broadcast(new BloomFilter(Integer.MAX_VALUE, 16, Hash.MURMUR_HASH))
    val b = sqlC.sparkContext.broadcast(new java.util.HashMap[String, BloomFilter]())

    broadcast.value.add(new Key("asd".getBytes()) )
    broadcast.value.add(new Key("a2sd".getBytes()) )

    var df = sqlC.createDataFrame(
      List(new UuidStat("uuid1", 1))
    )

    var df2 = sqlC.createDataFrame(
      List(new UuidStat("uuid2", 1))
    )

    b.value.put("xa",new BloomFilter(Integer.MAX_VALUE, 16, Hash.MURMUR_HASH))

    broadcast.value.add(new Key("asd".getBytes()))

    var _broadcast = broadcast;
    var _b = b;

//    var bb= (0 until 2000000).map{x=>
//      ("2", new Key(x.toString.getBytes())  )
//    }
//
//    bb ++
//    (0 until 2000000).map{x=>
//      ("2", new Key(x.toString.getBytes())  )
//    }
//    Thread.sleep(Long.MaxValue)

    //b_date <BloomFilter, ids>

    var b2:RDD[  (String, BloomFilter)  ] = sqlC.sparkContext.parallelize(Seq( ("2", new BloomFilter(Integer.MAX_VALUE, 16,1)) ))
    var b3:RDD[  (String, Key)  ] = sqlC.sparkContext.parallelize(
      (0 until 11).map{x=>
        ("2", new Key(x.toString.getBytes())  )
      }
      ++
      (0 until 11).map{x=>
        ("2", new Key(x.toString.getBytes()))
      }
      //Seq( ("2014", "asd" ),("2014", "asd" ),("2014", "asd" ),("2014", "asd" ) )
    )

    var b4= b2.join(b3)
    var b5=b4.filter{x=>
      var b=  x._2._1.membershipTest(x._2._2)
      println(x._2._1.hashCode())
      if(b) {
         false
      }else {
        x._2._1.add( x._2._2 )
         true
      }
    }.count()

    println("b5: " + b5)


    val bf= sqlC.sparkContext.parallelize(List(
      new BloomFilter(),new BloomFilter()
    ))

    println(bf.repartition(1).count()+" :count")

    bf.persist(StorageLevel.DISK_ONLY)

    println(OM.toJOSN(System.getProperties))




    df.rdd.map{x=>
      println(_broadcast.value)
      println(ll)
      println(_broadcast.value.membershipTest(new Key("asd".getBytes())))
      println(_broadcast.value.membershipTest(new Key("a2sd".getBytes())))
      println(_broadcast.value.membershipTest(new Key("a3sd".getBytes())))
      println(_b.value.get("xa") + "===")

      //      df2.show()
      x
    }.collect()

    df.show()
    Thread.sleep(Long.MaxValue)
  }
}
object AS3 {

  private var ll ="cccccccccc"

  def main3 (sqlC: SQLContext): Unit = {
    new AS3(sqlC).m()
  }
}
