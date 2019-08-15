package com.mobikok.ssp.data.streaming.util

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Date, TimeZone}

import com.mobikok.ssp.data.streaming.util.CSTTime.formatter
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.lock


//北京时区
object CSTTime {

  private val DF_yyyyMMddHHmmss = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyyMMddHHmmss")
  }

  private val lock = new Object()

  def uniqueSecondes (): Date ={
    lock.synchronized{
      Thread.sleep(1000)
      return new Date()
    }
  }

  def uniqueMS (): Date ={
    lock.synchronized{
      Thread.sleep(1)
      return new Date()
    }
  }

  def uniqueMsAsFormated (): String ={
    MS_TIME.get().format(uniqueMS())
  }

  def uniqueSecondesAsFormated (): String ={
    DF_yyyyMMddHHmmss.get().format(uniqueSecondes())
  }
  def uniqueSecondesAsFormatedNumber (): Long ={
    java.lang.Long.parseLong(DF_yyyyMMddHHmmss.get().format(uniqueSecondes()))
  }

  private val DF_HOUR = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("HH")
  }
  private val DF_DATE = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyy-MM-dd")
  }
  private val DF_TIME = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyy-MM-dd HH:mm:ss")
  }

  private val B_TIME = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyy-MM-dd HH:00:00")
  }

  private val MS_TIME = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyyMMddHHmmssSSS")
  }

  def dateFormatter(): DateFormat ={
    DF_DATE.get()
  }

  def timeFormatter(): DateFormat ={
    DF_TIME.get()
  }

  def formatter (format: String): SimpleDateFormat = {
    val df = new SimpleDateFormat(format)
    df.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    df
  }

  def date(date: Date): String  = {
    DF_DATE.get().format(date)
  }

  def time(time: Date): String  = {
    DF_TIME.get().format(time)
  }

  def time(time: Long): String  = {
    DF_TIME.get().format(new Date(time))
  }

  def date2TimeStamp(date_str: String/*, format: String*/): Long ={
    DF_TIME.get().parse(date_str).getTime
  }

  def ms(time: String): Long ={
    DF_TIME.get().parse(time).getTime
  }


//  def timeObject(time: Long): Date  = {
//    new Date(time)
//  }

  object now{
    def time(): String = {
      DF_TIME.get().format(new Date())
    }

    def ms(): java.lang.Long = {
      new Date().getTime
    }

    /** 推荐用 modifyHourAsDate */
    @deprecated
    def addHourToDate(hour: Double):String={
      DF_DATE.get().format(new Date(new Date().getTime + (hour*1000*60*60).toLong))
    }

    def modifyHourAsDate(hour: Double):String={
      DF_DATE.get().format(new Date(new Date().getTime + (hour*1000*60*60).toLong))
    }

    def modifyHourAsBTime(hour: Double):String={
      B_TIME.get().format(new Date(new Date().getTime + (hour*1000*60*60).toLong))
    }

    def modifyMinuteAsTime(Minute: Double):String={
      DF_TIME.get().format(new Date(new Date().getTime + (Minute*1000*60).toLong))
    }

    def offset(ms: Long):String={
      DF_TIME.get().format(new Date(new Date().getTime + ms))
    }
    def offset(ms: Long, foramt: String): String={
      formatter(foramt).format(new Date(new Date().getTime + ms))
    }

    def date(): String = {
      DF_DATE.get().format(new Date())
    }

    def hour(): Integer = {
      Integer.valueOf(DF_HOUR.get().format(new Date()))
    }

    def format(format: String): Integer = {
      Integer.valueOf(formatter(format).format(new Date()))
    }


    /** 推荐用 modifyHourAsTime */
    @deprecated
    def addHourToBTime(hour: Double):String={
      B_TIME.get().format(new Date(new Date().getTime + (hour*1000*60*60).toLong))
    }
//    def lastTime(): Integer = {
//      Integer.valueOf(B_TIME.get().format(new Date()))
//    }
  }

  private val DF_DATE_00_00_00 = new ThreadLocal[DateFormat]() {
    override protected def initialValue = formatter("yyyy-MM-dd 00:00:00")
  }

  /**
    * @param time 被分组的时间
    * @param spanHour 分组时间跨度，eg: 值为3，表示每3小时一个组, 从0点开始
    * @return 所在组的时间
    */
  def groupedTime(time:String, spanHour: java.lang.Double): String = {

    val intervalMS = 1000L * 60 * 60 * 24 / (24 / spanHour)
//    System.out.println("interMS: " + intervalMS)
    val startDayMS = DF_DATE_00_00_00.get().parse(time.split(" ")(0) + " 00:00:00").getTime
//    System.out.println("startDayMS: " + CSTTime.time(new Nothing(startDayMS)))
    val currentTimeMS = DF_TIME.get().parse(time).getTime
//    System.out.println("currentTime: " + CSTTime.time(new Nothing(currentTimeMS)))
    val nowDayMS = currentTimeMS - startDayMS
//    System.out.println("nowDayMS: " + CSTTime.time(new Nothing(nowDayMS)) + " (" + nowDayMS + ")")
    //CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse("1970-01-01 00:00:00").getTime() +
    val pos = Math.ceil(nowDayMS / intervalMS)
//    System.out.println("pos: " + pos)
    val offsetMS = (1000L * 60 * 60 * (pos - 1) * spanHour).toLong
    CSTTime.time(new Date(startDayMS + offsetMS))
//    System.out.println(CSTTime.time(new Date(startDayMS + offsetMS)))
  }

  /**
    * @param time     当前时间，格式为：yyyy-MM-dd HH:mm:ss
    * @param spanHour 分组时间跨度，eg: 值为3，表示每3小时一个组, 从0点开始
    * @param parts    
    * @return
    */
  def neighborTimes(time:String, spanHour: java.lang.Double, parts: Integer): Array[String] ={
//    val b_time = "2018-12-12 11:13:44"
//    val spanHour = 2
//    val parts = 1

    var result = new util.ArrayList[String]()
    val d = CSTTime.formatter("yyyy-MM-dd HH:mm:ss").parse(time)

    var i = 0 - parts
    while ( {i < parts + 1}) {

      val t = d.getTime + (1000L * 60 * 60 * spanHour * i).toLong
      result.add(CSTTime.time(new Date(t)))

      {i += 1}
    }
    result.toArray(new Array[String](0))
  }


  //TEST
  def main (args: Array[String]): Unit = {

    println(CSTTime.now.addHourToDate(-8*24))

//
//    for(i <- 0  until 12) {
//      new Thread(new Runnable {
//        override def run (): Unit = {
//          println( TimeUtil.uniqueSecondesAsFormatedNumber())
//        }
//      }).start()
//
//    }
  }

}


