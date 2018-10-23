package com.mobikok.ssp.data.streaming.module.support

import com.mobikok.ssp.data.streaming.udf._
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/12/20.
  */
object SQLContextGenerater {

  def generate(sparkContext: SparkContext): SQLContext = {
    val sc = new SQLContext(sparkContext)

    val bk = new UserAgentBrowserKernelUDF
    var dt = new UserAgentDeviceTypeUDF
    var os = new UserAgentOperatingSystemUDF
    var lang = new UserAgentLanguageUDF
    var mm = new UserAgentMachineModelUDF

    sc.udf.register("browserKernel", {x:String => bk.evaluate(new Text(x))})
    sc.udf.register("deviceType", {x:String  => dt.evaluate(new Text(x))})
    sc.udf.register("language", {x:String  => lang.evaluate(new Text(x))})
    sc.udf.register("machineModel", {x:String  => mm.evaluate(new Text(x))})
    sc.udf.register("operatingSystem", {x:String  => os.evaluate(new Text(x))})
    sc
  }
}
