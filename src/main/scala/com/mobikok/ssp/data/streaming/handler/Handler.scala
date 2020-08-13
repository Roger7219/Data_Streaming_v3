package com.mobikok.ssp.data.streaming.handler

trait Handler{

  var isAsynchronous = false // 默认是同步执行, false:同步, true:异步

}
