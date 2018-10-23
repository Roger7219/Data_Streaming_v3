package com.mobikok.ssp.data.streaming.client

import java.util.Random

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/3/3.
  */
class Cleanable {

  private var actions: ListBuffer[ Unit=>Any] = ListBuffer[Unit=> Any]()

  def addAction(action: =>Any): Cleanable = {
    actions.append({Unit=> action})
    this
  }

  def doActions(): Unit ={
    actions.foreach{x=>
      x.apply()
    }
    actions = ListBuffer[Unit=> Any]()
  }

}


object CleanableTest{

  def main (args: Array[String]): Unit = {
//    val s= new Cleanable(){{}}
//      .addAction({println("asd")})
//      .addAction(println("we"))
//      .doActions()

    val rand = new Random
    println(rand.nextInt(50))

  }
}
