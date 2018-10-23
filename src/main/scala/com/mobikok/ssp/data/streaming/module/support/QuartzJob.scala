package com.mobikok.ssp.data.streaming.module.support

import com.mobikok.ssp.data.streaming.module.QuartzModule
import com.mobikok.ssp.data.streaming.util.RunAgainIfError
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}

/**
  * Created by Administrator on 2018/9/12 0012.
  */
@DisallowConcurrentExecution
class QuartzJob extends Job{

  def execute(c: JobExecutionContext): Unit = {
    val m = c.getJobDetail
      .getJobDataMap
      .get(QuartzJob.QUARTZ_MODULE)
      .asInstanceOf[QuartzModule]

//    RunAgainIfError.run({
      m.handler()
//    }, s"moduleName: ${m.moduleName}")

  }
}

object QuartzJob {
  var QUARTZ_MODULE = "quartz_module"

}
