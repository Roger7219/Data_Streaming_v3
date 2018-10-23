package com.mobikok.ssp.data.streaming

import java.util.regex.Pattern

/**
  * Created by Administrator on 2018/1/26.
  */
object PatternTest {

  def main (args: Array[String]): Unit = {
    val p= Pattern.compile("asd_.{8}$")
    val m= p.matcher("asd_20121203")
    if(m.matches()) {
      println(m.group(0))
    }
  }
}
