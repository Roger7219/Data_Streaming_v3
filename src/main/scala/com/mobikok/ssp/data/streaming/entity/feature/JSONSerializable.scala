package com.mobikok.ssp.data.streaming.entity.feature

import com.mobikok.ssp.data.streaming.util.OM

/**
  * Created by Administrator on 2017/6/20.
  */
trait JSONSerializable {

  override def toString: String = {
    return OM.toJOSN(this)
  }
}
