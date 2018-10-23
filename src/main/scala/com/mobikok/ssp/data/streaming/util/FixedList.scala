package com.mobikok.ssp.data.streaming.util

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by Administrator on 2018/5/9.
  */
class FixedList[V](fixedSize: Integer)  {

  @volatile private var list: util.List[V] = new util.ArrayList[V]()
  //  private var fixedSize: Long = 0

  //  def this(fixedSize: Long) {
  //    this()
  //    this.fixedSize = fixedSize
  //  }

  def get(): util.List[V] ={
    synchronizedCall(new util.ArrayList[V](list))
  }

  def synchronizedCall[T](func: => T): T ={
    list.synchronized{
      func
    }
  }

  def add(value: V):Unit ={
    synchronizedCall{

      list.add(value)
      var removeCount = list.size() - fixedSize
      var i = 0
      if(removeCount > 0) {
        val needRemoveKeys = new util.ArrayList[V](removeCount)
        val iter = list.iterator()

        while(iter.hasNext && i < removeCount) {
          needRemoveKeys.add(iter.next())
          i = i + 1
        }

        needRemoveKeys.foreach{x=>
          list.remove(x)
        }
      }

    }
  }

  override def toString: String = list.toString

}

object FixedListTest {
  def main (args: Array[String]): Unit = {
    val fl = new FixedList[String](2)
    fl.add("3asd")
    fl.add("2asd")
    fl.add("1asd")
//    fl.add("0asd")
    fl.add("0asd")
    println(fl.get())
//
//    println( Array(1,3).reduce(_+_))


  }
}