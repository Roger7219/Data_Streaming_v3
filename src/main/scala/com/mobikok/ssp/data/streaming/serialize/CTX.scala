package com.mobikok.ssp.data.streaming.serialize

/**
  * Created by Administrator on 2017/6/16.
  */
class CTX[T](o:T) extends Serializable{

  def get(): T = o

}

object X{

  def main (args: Array[String]): Unit = {
    println(Array(1).isEmpty)
  }
}