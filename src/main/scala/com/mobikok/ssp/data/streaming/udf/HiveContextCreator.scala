package com.mobikok.ssp.data.streaming.udf

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/12/20.
  */
object HiveContextCreator {

  def create(sparkContext: SparkContext): HiveContext = {
    val hc = new HiveContext(sparkContext)

    val bk = new UserAgentBrowserKernelUDF
    val dt = new UserAgentDeviceTypeUDF
    val os = new UserAgentOperatingSystemUDF
    val lang = new UserAgentLanguageUDF
    val mm = new UserAgentMachineModelUDF

    hc.udf.register("browserKernel", {x:String => bk.evaluate(new Text(x))})
    hc.udf.register("deviceType", {x:String => dt.evaluate(new Text(x))})
    hc.udf.register("operatingSystem", {x:String => os.evaluate(new Text(x))})
    hc.udf.register("language", {x:String => lang.evaluate(new Text(x))})
    hc.udf.register("machineModel", {x:String => mm.evaluate(new Text(x))})

    hc
  }

}
