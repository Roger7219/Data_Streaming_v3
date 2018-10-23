package com.mobikok.ssp.data.streaming.util

import java.util
import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/1/29.
  */
object RegexUtil {

  def matched(inputText: String, pattern: String): List[String] ={
    try{
      val res = ListBuffer[String]()
      val p = Pattern.compile(pattern);
      val m = p.matcher(inputText);
      while(m.find()) {
        res.append(m.group())
      }
      res.toList
    }catch {case e:Throwable=>
        throw new RuntimeException(s"Regex match exception, inputText: $inputText, pattern: $pattern", e)
    }
  }

  def matchedGroups(inputText: String, pattern: String): List[String] ={
    try {
      val res = ListBuffer[String]()
      val p = Pattern.compile(pattern);
      val m = p.matcher(inputText);
      while(m.find()) {
        if(m.groupCount() == 0) {
          res.append(m.group())
        }else {
          (0 until m.groupCount()).foreach{x=>
            res.append(m.group(x + 1))
          }
        }
      }
      res.toList
    }catch {case e:Throwable=>
      throw new RuntimeException(s"Regex match exception, inputText: $inputText, pattern: $pattern", e)
    }
  }

  def matchedGroups(inputText: String, pattern: String, patterns: String*): List[String] ={
    var res = ListBuffer[String]()
    val ps = pattern +: patterns
    var inps = ListBuffer[String](inputText)

    var inps2 = ListBuffer[String]()

    ps.foreach{x=>
      res = ListBuffer[String]()
      inps2 = ListBuffer[String]()

      inps.foreach{y=>
        inps2 ++= matchedGroups(y, x)
      }
      inps = inps2
      res = inps
    }

    res.toList
  }


  def main (args: Array[String]): Unit = {
//    println(RegexUtil.matchedGroups("我的QQ是:456456 我的电话是:0532214 我的邮箱是:aaa123@aaa.com", "(\\d+).*?"))
//    println(RegexUtil.matchedGroups("SELECT * FROM test s union select * from dual as s", "(?i)from\\s+([\\S]*)"))
//
  var s = "\000"
      RegexUtil.matchedGroups(
      s"""
        |SELECT
        | f1,
        | sum(f2) as f2,
        | f3 as f3
        | FROM test s
         """
      .stripMargin.replaceAll("\\s*,\n", s),
      "(?i)SELECT\\s*(.*?)\\s+FROM", s"\\s*([^$s]+)"
     ).foreach(println(_))

    println("---------------")
    RegexUtil.matchedGroups(
      s"""
         |SELECT
         | f1,
         | sum(f2) as f2,
         | f3 as f3
         | FROM test s
         """
        .stripMargin.replaceAll("\\s*,\n", s),
      "(?i)select\\s*(.*?)\\s+from", s"\\s*([^$s]+)", "\\s*(\\S+)$"
    ).foreach(println(_))


    println("---------------")
    RegexUtil.matchedGroups(
      s"""
         |SELECT f1,
         | f2 as f2,
         |f3 as f3
         | FROM test s
         | group by
         | s2,
         | s3 as s3,
         | s4"""
        .stripMargin.replaceAll("\\s*,\n", s),
      "(?i)group by\\s*(.*?)\\s?$", s"\\s*([^$s]+)"
      ).foreach(println(_))


  }

}
