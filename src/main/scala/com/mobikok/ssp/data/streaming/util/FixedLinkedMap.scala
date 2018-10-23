package com.mobikok.ssp.data.streaming.util

import java.util
import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2018/5/8.
  */
class FixedLinkedMap[K,V](fixedSize: Integer) {

  @volatile private var linkedMap: util.Map[K, V] = new util.LinkedHashMap[K, V]()
//  private var fixedSize: Long = 0

//  def this(fixedSize: Long) {
//    this()
//    this.fixedSize = fixedSize
//  }

  def get(key:K): V ={
    linkedMap.get(key)
  }

  def put(key: K, value: V):Unit ={
    linkedMap.synchronized{

      linkedMap.put(key, value)
      var removeCount = linkedMap.size() - fixedSize
      var i = 0
      if(removeCount > 0) {
        val needRemoveKeys = new util.ArrayList[K](removeCount)
        val iter = linkedMap.entrySet().iterator()

        while(iter.hasNext && i < removeCount) {
          needRemoveKeys.add(iter.next().getKey)
          i = i + 1
        }

        needRemoveKeys.foreach{x=>
          linkedMap.remove(x)
        }
      }

    }
  }

  override def toString: String = linkedMap.toString

}
object FixedLinkedMapTest{

  def main (args: Array[String]): Unit = {
    val map = new FixedLinkedMap[String, String](2)
    map.put("zsd","")
    map.put("df","")
    map.put("ee","")
    map.put("aw","")
    map.put("ee","")
    println(map)

    println(new Integer(11)  == new Integer(11))
  }
}
