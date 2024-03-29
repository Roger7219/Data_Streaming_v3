package com.mobikok.ssp.data.streaming.util

import java.{lang, util}
import java.util.List

import com.mobikok.message.{Message, Resp}
import JavaMessageClient.Callback
import com.mobikok.message.client.MessageClientApi

/**
  * Created by admin on 2018/1/19.
  */
object MessageClientTest {

  def main(args: Array[String]): Unit = {

    val topic = Array("report_campaign_fill", "ssp_report_campaign_dwr","new_ssp_image_dm_update")
    val mc = new MessageClientApi("http://node14:5555")
    JavaMessageClient.pullAndCommit(mc, "new_ssp_image_dm_bqcer", new Callback[Resp[util.List[Message]]] {
      override def doCallback(resp: Resp[util.List[Message]]): lang.Boolean = true
    } , topic:_*)
  }
}
