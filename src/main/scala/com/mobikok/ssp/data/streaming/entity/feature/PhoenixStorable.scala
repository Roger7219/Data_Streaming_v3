package com.mobikok.ssp.data.streaming.entity.feature

import java.util

import org.apache.hadoop.hbase.util.Bytes

import collection.JavaConverters._
import collection.JavaConversions._
object PhoenixStorablet{

  def ifNull[T] (o: Array[Byte], a: => T, b: => T): T = {
    if(o == null) a else b
  }
  def main (args: Array[String]): Unit = {
    println( ifNull(null, null,  2))
  }
}
/**
  * 只支持String,Long,Integer类型的字段！！
  *
  * Created by Administrator on 2017/6/23.
  */
trait PhoenixStorable extends HBaseStorable {

  //integer
  protected def getInt(source: collection.Map[(String, String), Array[Byte]], fieldName: String): Integer ={
    ifNull(source.get(("0", fieldName)).get, null,  Bytes.toInt(source.get(("0", fieldName)).get) + Int.MaxValue + 1)
  }
  //string
  protected def getStr(source: collection.Map[(String, String), Array[Byte]], fieldName: String): String ={
    ifNull(source.get(("0", fieldName)).get, null, Bytes.toString(source.get(("0",fieldName)).get))
  }
  //double
  protected def getDou(source: collection.Map[(String, String), Array[Byte]], fieldName: String): java.lang.Double = {
    ifNull(source.get(("0", fieldName)).get, null, java.lang.Double.valueOf( Bytes.toString(source.get(("0", fieldName)).get)))
  }

  def ifNull[T] (o: Array[Byte], a: => T, b: => T): T = {
    if(o == null) a else b
  }

  def ifNull (o: Any, a: => Array[Byte], b: => Array[Byte]): Array[Byte] = {
    if(o == null)  a else b
  }

  //integer
  def setInt(value: Integer): Array[Byte] = {
    ifNull(value, null, Bytes.toBytes(value - Int.MaxValue - 1))
  }
  //string
  def setStr(value: String):Array[Byte] = {
    ifNull(value, null, Bytes.toBytes(value))
  }
  //double
  def setDou(value: java.lang.Double): Array[Byte] = {
    ifNull(value, null, Bytes.toBytes(value  + ""))
  }

  @deprecated
  protected def parseRowkey (key: Array[Byte], clazz: Class[_], clazzs: Class[_]*): Array[_] = {

    val cs =   clazz +: clazzs
    val result = new Array[Any](cs.size)

    var start = 0
    var end = 0

    for (i <- 0 until cs.length) {
      val x = cs(i)
      var v: Any = null

      if (x == classOf[Int]) {
        end += 4
        v = Bytes.toInt(util.Arrays.copyOfRange(key, start, end)) + Int.MaxValue + 1

      } else if (x == classOf[Long]) {
        end += 8
        v = Bytes.toLong(util.Arrays.copyOfRange(key, start, end)) + Long.MaxValue + 1

      } else if (x == classOf[String]) {

        var cycle = true
        for (i <- end until key.length if cycle) {
          end = i + 1
          if (key(i) == '\u0000') {
            cycle = false
          }
        }
        v = Bytes.toString(util.Arrays.copyOfRange(key, start, if(cycle) end else  end - 1))

      }else {
        throw new UnsupportedOperationException(s"PhoenixStorable instance field type '$x' is not supported, Only Int, Long, String is supported.")
      }

      result.update(i, v)
      start = end

    }
    result
  }
}




















//object x{
//  def main (args: Array[String]): Unit = {
//
//   val  xx=new Array[Object](3)
//    xx.update(1,"123")
//    println(xx.toSeq ++ "123"
//    )
//
//    println(Double.MaxValue-1.988465674311579E307)
//  }
//}
