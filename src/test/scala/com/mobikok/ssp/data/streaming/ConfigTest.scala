package com.mobikok.ssp.data.streaming

import java.io.File
import java.util.Date

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2017/7/17.
  */
object ConfigTest {

  def main (args: Array[String]): Unit = {
//    var config: Config = ConfigFactory.parseFile(new File("C:\\Users\\Administrator\\IdeaProjects\\datastreaming\\src\\main\\scala\\dw\\image.conf"))
//
//    config.getConfigList("modules.image_show.dm.handler.setting").foreach{x=>
//      println(x.getStringList("params")(0))
//    }

    println(new Date().getHours)


  }
}
