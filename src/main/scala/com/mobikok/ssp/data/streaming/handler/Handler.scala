package com.mobikok.ssp.data.streaming.handler

trait Handler {

  var isAsynchronous = false // 表示该handler是否异步执行,false：同步，true：异步

}
