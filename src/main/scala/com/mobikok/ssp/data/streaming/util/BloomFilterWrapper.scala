package com.mobikok.ssp.data.streaming.util

import org.apache.hadoop.util.bloom.{BloomFilter, Key}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/3/8 0008.
  */
class BloomFilterWrapper{

  private val bloomFilters = ListBuffer[BloomFilter]()



  def append(appendBloomFilter: BloomFilter): Unit ={
    bloomFilters.append(appendBloomFilter)
  }

  def membershipTest(key: Key): Boolean = {

    // false表示不在集合中
    var result = false

    bloomFilters.par.foreach{x=>
      var r = x.membershipTest(key)
      bloomFilters.synchronized{
        if(!result) {
          result = r
        }
      }
    }
    result
  }
}
