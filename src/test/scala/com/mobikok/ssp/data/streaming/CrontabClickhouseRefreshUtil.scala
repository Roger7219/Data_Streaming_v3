package com.mobikok.ssp.data.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.MessagePushReq
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.config.DynamicConfig
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.{CSTTime, MC, OM}

/**nadx_p_matched_dwi_cer
  * Created by Administrator on 2017/9/26.
  */
object CrontabClickhouseRefreshUtil {

  val DF = new SimpleDateFormat("yyyy-MM-dd")
  val messageClient = new MessageClient("", "http://node14:5555")

  def main (args: Array[String]): Unit = {


//    http://node14:5555//Api/Message?consumer=nadx_p_matched_dwi_cer&topics=nadx_performance_dwi&topics=nadx_traffic_dwi
//    messageResetToLastest("nadx_p_matched_dwi_cer", Array("nadx_performance_dwi", "nadx_traffic_dwi"))
//    messageResetToLastest("nadx_p_matched_dwi_cer", Array("nadx_overall_traffic_dwi"))


    // nadx ck 重刷指定b_time区间的
//    messageResetToLastest("nadx_overall_dm_cer", Array("nadx_overall_dwr"))

//      sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-17 12:00:00", "2019-07-17 12:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-25 00:00:00", "2019-03-25 23:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-24 00:00:00", "2019-03-24 23:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-26 00:00:00", "2019-03-26 23:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-27 00:00:00", "2019-03-27 23:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-28 00:00:00", "2019-03-28 23:00:00", "nadx_ck")

//    sendMsg_btimes_for_ck("dsp_cloak_stat_detail_dwr", "2019-05-25 00:00:00", "2019-05-25 23:00:00", "nadx_ck")


    //未执行
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-03-23 00:00:00", "2019-03-23 23:00:00", "nadx_ck")

//    println(OM.toJOSN(new MessagePushReq("topic1", "data")))

    // 注意，配置里配置了最早能刷新过去36小时的数据
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 11:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 12:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 13:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 14:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 15:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 18:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 22:00:00")

//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 00:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 07:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 08:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 10:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 13:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-04 20:00:00")


//    messageResetToLastest("nadx_overall_dm_v6_cer", Array("nadx_overall_dwr_v6"))
//    messageResetToLastest("nadx_overall_traffic_audit_cer", Array("nadx_overall_audit_dwr"))

//    sendMsg_btime("nadx_performance_dwi", "2019-06-02 23:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 15:00:00")
//    sendMsg_btime("nadx_performance_dwi", "2019-06-03 16:00:00")


//        var b_date =args(0);
//    println("Refresh start::::::::::::: " + b_date)
//    sendMsg_btimes_for_ck("nadx_overall_dwr", b_date + " 00:00:00", b_date + " 23:00:00", "nadx_ck")
//    println("Refresh done::::::::::::: " + b_date)

    //    sendMsg_btime_AllHours("update_ssp_report_overall", "2018-05-02", "2018-05-02")
//    Thread.sleep(1000*60*60*1)
//    sendMsg_btime_AllHours("update_ssp_report_overall", "2018-01-15", "2018-01-18")
//    sendMsg("PublisherThirdIncomeDMReflush", "2017-12-14")
//    sendMsg("PublisherThirdIncomeDMReflush", "2017-12-15")
//    sendMsg("PublisherThirdIncomeDMReflush", "2017-12-16")
//    sendMsg("PublisherThirdIncomeDMReflush", "2017-12-17")

//ssp_report_overall
//    sendMsg_btime("update_ssp_report_overall",400)
//    sendMsg_btime_AllHours("update_ssp_report_overall", "2018-01-14", "2018-01-15")
//    sendMsg_btime("update_ssp_report_overall", "2018-01-16 19:00:00")
//

//    sendMaxWaitingTimeMS(DynamicConfig.of("bq_report_mix", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*4)) // 4小时
//    sendMsg_btime_AllHours("update_ssp_report_overall", "2018-01-15", "2018-01-16")
//    sendMsg_btime_AllHours("update_ssp_report_overall", "2018-01-16","2018-01-17")
//    sendMsg_btime_AllHours("update_ssp_report_overall","2018-01-18", "2018-01-19")
//    sendMsg_btime_00_00_00("update_ssp_report_overall","2016-10-07", "2017-12-18")
//    sendMsg_btime_AllHours("update_ssp_report_overall","2018-01-03", "2018-01-04")
//
//    sendMsg_btime("update_ssp_report_overall", "2018-03-29 02:00:00")
//    sendMsg_btime("update_ssp_report_overall", "2018-03-29 08:00:00")
//    sendMsg_btime("bq_report_overall_day_v2_tmp_update", "2018-06-13 00:00:00")
//
//    sendMsg_btime("ssp_report_overall_dm_day_v2_update", "2018-06-13 00:00:00")
//    sendMsg_btime("ssp_report_overall_dm_day_v2_update", "2018-06-14 00:00:00")

//    sendMsg_btime_00_00_00("bq_report_overall_day_v2_tmp_update", "2018-04-01",  "2018-04-15")
//    sendMsg_btime("bq_report_overall_day_v2_tmp_update", "2018-04-01 00:00:00")

//        sendMaxWaitingTimeMS(DynamicConfig.of("bq_report_mix", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*24)) // 24小时
//    sendMsg_btime_AllHours("update_ssp_report_overall","2018-02-01", "2018-02-05")

    //
//    sendMsg_btime_00_00_00("update_ssp_report_overall","2017-11-28", "2017-11-29")


//    sendMsg_btime("update_ssp_report_overall", "2019-03-22 01:00:00")


//    sendMsg_btime("update_ssp_report_overall", "2018-01-04 05:00:00")
//    sendMsg_btime("update_ssp_report_overall", "2018-01-04 07:00:00")
//    sendMsg_btime("update_ssp_report_overall", "2018-01-04 17:00:00")

    ////    agg_traffic 2
////    app 2
////    dsp 2
////    dupscribe_and_dupscribe_detail 2
////
////    image 1
//    log
////    offer 3
//    report_campaign
//    sendMsg_b_date("report_campaign_fill", "2017-01-15")
//    sendMsg_b_date("report_campaign_fill", "2017-01-16")
//    sendMsg_b_date("report_campaign_fill", "2017-01-18")
//    sendMsg("report_campaign_fill", "2017-12-18")
    //    ////    sendMsg("report_campaign_fill", "2017-10-01")
//////    sendMsg("report_campaign_fill", "2017-10-02")
//////    sendMsg("report_campaign_fill", "2017-10-03")
//    report_publisher
//    sendMsg_b_date("report_publisher_fill", "2017-10-15")
//    sendMsg_b_date("report_publisher_fill", "2017-10-16")
//    sendMsg_b_date("report_publisher_fill", "2017-10-18")
////    sendMsg("report_publisher_fill", "2017-10-01")
////    sendMsg("report_publisher_fill", "2017-10-02")
////    sendMsg("report_publisher_fill", "2017-10-03")
//    topOffer
////    sendMsg("offer_click", "2017-10-01")
////    sendMsg("offer_click", "2017-10-02")
////    sendMsg("offer_click", "2017-10-03")
////      sendMsg("report_campaign_fee", "2017-11-10")
//      topn
////    bd_offer
//////
//    user_keep
//    user_na
////    sendMsg("user_new", "2017-10-01")
////    sendMsg("user_new", "2017-10-02")
////    sendMsg("user_new", "2017-10-03")
////      adx()
//
    // 重刷离线统计监控结果
//    sendMsg_b_date("ssp_fee_dwi", "2018-03-28")
//    sendMsg_b_date("ssp_fee_dwi", "2018-03-27")



//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-04-26")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-04-27")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-04-28")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-04-29")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-04-30")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-05-01")
//    sendMsg_b_date("ssp_report_overall_dwr_day", "2018-05-02")
//    sendMsg_b_date("ck_report_overall_day", "2018-08-14")


//    sendMsg_btimes_for_ck("nadx_performance_dwi", "2019-08-06 09:00:00", "2019-08-06 09:00:00", "adx_dwr_v6")
//    sendMsg_btimes_for_ck("nadx_performance_dwi", "2019-08-06 07:00:00", "2019-08-06 07:00:00", "adx_dwr_v6")
//    sendMsg_btimes_for_ck("nadx_performance_dwi", "2019-08-05 11:00:00", "2019-08-05 11:00:00", "adx_dwr_v6")
//    sendMsg_btimes_for_ck("nadx_performance_dwi", "2019-08-05 03:00:00", "2019-08-05 03:00:00", "adx_dwr_v6")

//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6", "2019-07-31 17:00:00", "2019-08-02 19:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6", "2019-06-29 00:00:00", "2019-06-29 00:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6", "2019-06-29 21:00:00", "2019-06-29 21:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6", "2019-06-29 19:00:00", "2019-06-29 19:00:00", "nadx_ck")


    // 刷新adx ck数据--------------------------------------------------
    sendMsg_btimes_for_nadx("nadx_overall_dwr",    "2019-08-21 19:00:00",    "2019-08-21 20:00:00", "nadx_ck")
    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")
    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")

    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_1",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")
    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_1",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")

    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_2",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")
    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_2",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")

    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_3",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")
    sendMsg_btimes_for_nadx("nadx_overall_dwr_v6_3",    "2019-08-21 19:00:00", "2019-08-21 20:00:00", "nadx_ck")

//    -----------------

//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6",    "2019-08-14 22:00:00", "2019-08-14 22:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6_1",    "2019-08-14 22:00:00", "2019-08-14 22:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6_2",    "2019-08-14 22:00:00", "2019-08-14 22:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6_3",    "2019-08-14 22:00:00", "2019-08-14 22:00:00", "nadx_ck")

//      sendMsg_btimes_for_ck("nadx_overall_dwr_v6",    "2019-08-16 03:00:00", "2019-08-16 10:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr_v6_1",    "2019-08-16 03:00:00", "2019-08-16 10:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr_v6_2",    "2019-08-16 03:00:00", "2019-08-16 10:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr_v6_3",    "2019-08-16 03:00:00", "2019-08-16 10:00:00", "nadx_ck")

//    sendMsg_btimes_for_nadx("nadx_traffic_dwi", "2019-08-19 08:00:00", "2019-08-19 08:00:00", "adx_dwr_v6")

    //    "2019-08-13 13:00:00",
//    "2019-08-13 12:00:00",
//    "2019-08-13 11:00:00",
//    "2019-08-13 10:00:00",
//    "2019-08-12 19:00:00",
//    "2019-08-12 18:00:00",
//    "2019-08-12 17:00:00",
//    "2019-08-12 16:00:00",
//    "2019-08-12 15:00:00",
//    "2019-08-12 14:00:00",
//    "2019-08-12 13:00:00",
//    "2019-08-12 12:00:00"

//    sendMsg_btimes_for_ck("nadx_overall_dwr",    "2019-07-27 17:00:00", "2019-07-27 17:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-29 17:00:00", "2019-07-29 17:00:00", "nadx_ck")

//    sendMsg_btimes_for_ck("nadx_overall_dwr_v6", "2019-07-23 00:00:00", "2019-07-23 18:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-23 21:00:00", "2019-07-23 21:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-10 00:00:00", "2019-07-10 23:00:00", "nadx_ck")
//      sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-17 12:00:00", "2019-07-17 12:00:00", "nadx_ck")
//    sendMsg_btimes_for_ck("nadx_overall_dwr", "2019-07-17 12:00:00", "2019-07-17 12:00:00", "nadx_ck")

    // 刷新campaign/publisher表
//    sendMsg_btime_00_00_00("ssp_report_overall_dwr_day", "2018-06-01", "2018-06-10")
//    sendMsg_btime_00_00_00("ssp_report_overall_dwr_day", "2018-05-01", "2018-05-31")

//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 12:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 13:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 14:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 15:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 16:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 17:00:00")
//    sendMsg_btime("ssp_report_overall_dwr", "2019-08-08 18:00:00")


    // 重刷28 29号的数据
//    sendMsg_btime_00_00_00("bq_report_campaign_update", "2018-06-27", "2018-06-29")
//    sendMsg_btime_00_00_00("bq_report_campaign_update", "2018-07-24", "2018-07-24")
//    sendMsg_btime_00_00_00("bq_report_publisher_update", "2018-07-24", "2018-07-24")


    //    sendMsg_btime_00_00_00("bq_report_publisher_update", "2018-06-27", "2018-06-29")
//    sendMsg_btime_00_00_00("bq_report_publisher_update", "2018-07-10", "2018-07-10")

    //
//    sendMsg_btime_00_00_00("ssp_report_overall_dwr_day", "2018-06-04", "2018-06-04")
//    sendMsg_btime_00_00_00("ssp_report_overall_dwr_day", "2018-06-12", "2018-06-12")

//        sendMsg_btime_00_00_00("ssp_report_overall_dm_day_v2_update", "2018-04-01", "2018-04-15")
//    sendMsg_btime("ssp_report_overall_dm_day_v2_update", "2018-04-01 00:00:00")
//    sendMsg_btime("ck_report_overall", "2018-12-03 12:00:00")

//    sendMsg_btime_00_00_00("ssp_report_overall_dwr_day", "2018-06-10", "2018-06-10")
//    sendMsg_l_time("ssp_report_overall_dwr_day_test", "2018-06-12 00:00:00")

//    26 27 28 29
//    sendMsg_btime_AllHours("update_ssp_report_overall2", "2018-06-26", "2018-06-30")
//  26
//    sendMsg_btime_AllHours("update_ssp_report_overall2", "2018-06-26", "2018-06-27")

    ////    sendMsg("click_ext", 47)
//    sendMsg("ssp_image_dm_update", "2018-01-18")

//    sendMsgDmTableGeneratorHandlerNeedInit("ssp_report_overall_dwr_month_re_init", "ssp_report_overall_dwr_month", "2018-04-26 00:00:00")
//    sendMsgDmTableGeneratorHandlerNeedInit("ssp_report_campaign_month_dm_re_init", "ssp_report_campaign_month_dm", "2018-03-28 00:00:00")
//   sendMsgDmTableGeneratorHandlerNeedInit("agg_traffic_month_dwr_re_init", "agg_traffic_month_dwr", "2018-02-04 00:00:00")

    //新增加速表初始化
//    sendMsgDmTableGeneratorHandlerNeedInit("ssp_report_overall_dwr_day_re_init", "ssp_report_overall_dwr_day", "ssp_report_overall_dwr_day")





    // overall for month init
//    sendMaxWaitingTimeMS(DynamicConfig.of("ssp_report_overall_dwr_month", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*100L)) // 4小时
//    sendMaxWaitingTimeMS(DynamicConfig.of("ck_overall_import_month", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*12L)) // 4小时
//    sendMaxWaitingTimeMS(DynamicConfig.of("hdfs_restore_to_hive", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*6L)) // 4小时
//    sendMaxWaitingTimeMS(DynamicConfig.of("bq_report_overall ", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*100L)) // 4小时
//    sendMaxWaitingTimeMS(DynamicConfig.of("ssp_send ", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*100L)) // 4小时

//    sendMaxWaitingTimeMS(DynamicConfig.of("bq_report_overall2", DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*100L)) // 4小时

//    nadx_performance_dwi", "nadx_traffic_dwi", "nadx_overall_traffic_dwi_v2
//    messageResetToLastest("nadx_p_matched_dwi_cer_v6", Array("nadx_performance_dwi"))
//    messageResetToLastest("nadx_p_matched_dwi_cer_v6", Array("nadx_traffic_dwi"))
//    messageResetToLastest("nadx_p_matched_dwi_cer_v6", Array("nadx_overall_traffic_dwi_v2"))
//      messageResetToLastest("ssp_report_publisher_dm_bqcer", Array("ssp_report_overall_dwr_day"))
// s"${view}_cer"
//    messageResetToLastest("nadx_overall_dm_day_cer", Array("nadx_overall_dwr_v6"))
//    messageResetToLastest("ssp_report_overall_dwr_day_cer_for_b_date", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("RedisDayMonthLimitHandler_cer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("OffferRoiEcpm2RedisHandler_cer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("ImageHandler_cer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("new_ssp_image_dm_bqcer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("offerHandler", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("nadx_p_matched_dwi_cer_v12", Array("nadx_performance_dwi", "nadx_traffic_dwi"))

    //    messageResetToLastest("writeToRedisCer", Array("ssp_report_overall_dwr"))
//      messageResetToLastest("bd_offer_dm_bqcer", Array("ssp_report_overall_dwr"))
    //    messageResetToLastest("monitor_offer_cer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("ssp_topn_dm_cer", Array("ssp_report_overall_dwr"))

//    messageResetToLastest("ssp_report_overall_dm_bqcer", Array("ssp_report_overall_dwr"))
//    messageResetToLastest("ssp_report_overall_dm_bqcer", Array("update_ssp_report_overall2"))


//    messageResetToLastest("ssp_report_campaign_dm_bqcer", Array("ssp_report_overall_dwr_day"))
//
//    messageResetToLastest("ssp_report_publisher_dm_bqcer", Array("ssp_report_overall_dwr_day"))


//    messageResetToLastest("kill_self_cer", Array("s__overall"))

//    killApp("overall_dwi")
//    killApp("overall")
//    killApp("overall")

//      killApp("adx_dwr_v20", "application_1561429996932_4476")

//    killApp("quartz_mix")
//    messageResetToLastest("bq_report_overall_day_v2_bqcer", Array("ssp_report_overall_dm_day_v2_update"))
//    messageResetToLastest("bq_report_overall_day_v2_bqcer_tmp", Array("bq_report_overall_day_v2_tmp_update"))
//    sendMsg_btime("ck_overall_import_month", "2016-10-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2016-11-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2016-12-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-01-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-02-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-03-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-04-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-05-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-06-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-07-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-08-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-09-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-10-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-11-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2017-12-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-01-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-02-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-03-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-04-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-05-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-06-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-07-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-08-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-09-01 00:00:00")
//    sendMsg_btime("ck_overall_import_month", "2018-10-01 00:00:00")

//    sendMsg_btime("ck_report_overall_day", "2018-12-01 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-02 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-03 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-04 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-05 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-06 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-07 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-08 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-09 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-10 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-11 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-12 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-13 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-14 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-15 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-16 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-17 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-18 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-19 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-20 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-21 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-22 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-23 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-24 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-25 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-26 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-27 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-28 00:00:00")
//    sendMsg_btime("ck_report_overall_day", "2018-12-29 00:00:00")



//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 00:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 01:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 02:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 03:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 04:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 05:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 06:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 07:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 08:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 09:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 10:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 11:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 12:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 13:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 14:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 15:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 16:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 17:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 18:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 19:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 20:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 21:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 22:00:00")
//    sendMsg_btime("hdfs_restore_to_hive", "2018-07-21 23:00:00")

//    sendMsg_btime("ck_report_overall", "2019-01-08 13:00:00")
//    sendMsg_btime("ck_report_overall_day", "2017-11-21 00:00:00")


//    sendMsg_btime_00_00_00("test__overall_history", "2018-09-01", "2018-09-15")
  }


  def messageResetToLastest(consumer: String, topics: Array[String]): Unit = {
    MC.init(messageClient)
    MC.pull(consumer,topics, {x=> true})
  }

  def sendMaxWaitingTimeMS(appName:String, ms: String): Unit ={
    messageClient.pushMessage(new MessagePushReq(appName, ms))
  }

  def killApp(appName:String, appId:String): Unit = {
    messageClient.pushMessage(new MessagePushReq("kill_self_"+appName,  appId))
  }


  def sendMsg(topic: String, days: Int): Unit = {
    var s = (0 until days).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_date", DF.format(new Date(new Date().getTime - x * 1000 * 60 * 60 * 24L))))))
      )
    }
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def sendMsg_btime(topic: String, days: Int): Unit = {
    var s = (0 until days).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_time", DF.format(new Date(new Date().getTime- x * 1000 * 60 * 60 * 24L)) + " 00:00:00"))))
      )
    }
    println(s)
//    messageClient.pushMessage(s: _*);
  }

  def daysBetween(startDay: String, endDay: String) = {
    var diff = 0
    if (startDay.equals(endDay)) {
      diff = 1
    } else {
      val difference = (DF.parse(endDay).getTime - DF.parse(startDay).getTime) / 86400000
      diff = difference.toInt
    }

    diff
  }

  //含startDay，不包含endDay
  def sendMsg_btime_AllHours (topic: String, startDay: String, endDay: String): Unit = {

    val days =daysBetween(startDay, endDay)
    val s = (0 until days).map { x =>
      (0 until 24).map{y=>
        var h = ""
        if(y<10) {
          h = s" 0${y}:00:00"
        }else {
          h = s" ${y}:00:00"
        }
        new MessagePushReq(
          topic,
          OM.toJOSN(Array(Array(HivePartitionPart("b_time", DF.format(new Date(DF.parse(startDay).getTime + x * 1000 * 60 * 60 * 24L)) + h))))
        )
      }
    }.flatMap{x=>x}
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def sendMsg_btime (topic: String, b_time: String): Unit = {

    val s = (0 until 1).map { x =>
      (0 until 1).map{y=>
        new MessagePushReq(
          topic,
          OM.toJOSN(Array(Array(HivePartitionPart("b_time", b_time))))
        )
      }
    }.flatMap{x=>x}
    println(s)
    messageClient.pushMessage(s: _*);
  }
  //ssp_report_campaign_month_dm_needInitBaseTable
  def sendMsgDmTableGeneratorHandlerNeedInit (topic: String, table:String, appName:String): Unit ={

    var yesterdayDateTime = CSTTime.now.offset(-1000*60*60*24, "yyyy-MM-dd 00:00:00")
      //CSTTime.formatter("yyyy-MM-dd 00:00:00").format(CSTTime.timeObject(CSTTime.now.ms() - 1000*60*60*24))
    messageClient.pushMessage(Array(new MessagePushReq(table + "_needInitBaseTable", "needInit")):_*)
    sendMsg_l_time(topic, yesterdayDateTime)
    // +1 天
    val f= CSTTime.formatter("yyyy-MM-dd HH:mm:ss")
    val incrStartTime = f.format(new Date(f.parse(yesterdayDateTime).getTime + 1000*60*60*24))
    sendMsg_l_time(topic, incrStartTime)

    sendMaxWaitingTimeMS(DynamicConfig.of(appName, DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*60*100L)) // 4小时
  }

  //不含startDate
  def sendMsg_btime_00_00_00 (topic: String, startDay: String, endDay: String): Unit = {

    val days =daysBetween(startDay, endDay)
    val s = (0 until days).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_time", DF.format(new Date(DF.parse(endDay).getTime - x * 1000 * 60 * 60 * 24L)) + " 00:00:00"))))
      )
    }
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def sendMsg_btimes_for_nadx(topic: String, startBTime: String, endBTime: String, appName: String): Unit = {

    var startT = CSTTime.ms(startBTime)
    var endT = CSTTime.ms(endBTime)
    var ts = endT - startT
    if(ts <0) {
      throw new RuntimeException(s"Requirement: startBTime <= endBTime, But actually: ${startBTime}(startBTime) > ${endBTime}(endBTime) ")
    }
    var hours = (ts/(1000 * 60 * 60)).toInt + 1
    val s = (0 until hours).map { x =>
      var t = startT + (x * 1000 * 60 * 60)

      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_time", CSTTime.time(t)))))
      )
    }
//    println(s)
    messageClient.pushMessage(s: _*);

    sendMaxWaitingTimeMS(DynamicConfig.of(appName, DynamicConfig.BATCH_PROCESSING_TIMEOUT_MS), String.valueOf(1000*60*5*hours*2)) // 4小时

  }


  def sendMsg_btime_00_00_00 (topic: String, days: Int, startDay: String): Unit = {
    var s = (0 until days).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_time", DF.format(new Date(DF.parse(startDay).getTime - x * 1000 * 60 * 60 * 24L)) + " 00:00:00"))))
      )
    }
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def sendMsg_b_date (topic: String, day: String): Unit = {
    var s = (0 until 1).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("b_date", day))))
      )
    }
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def sendMsg_l_time(topic: String, day: String): Unit = {
    var s = (0 until 1).map { x =>
      new MessagePushReq(
        topic,
        OM.toJOSN(Array(Array(HivePartitionPart("l_time", day))))
      )
    }
    println(s)
    messageClient.pushMessage(s: _*);
  }

  def bd_offer(): Unit = {
    sendMsg("report_campaign_fee", 4)
  }

  def topn(): Unit = {
    sendMsg("update_ssp_topn_dm", 5)
  }

  def adx(): Unit = {
    sendMsg("adx_ssp", 10)
  }


  def agg_traffic(): Unit = {
    sendMsg("agg_traffic_user_new", 67)
  }

  def app(): Unit = {
    sendMsg("app_totalcost_fee", 67)
  }

  def dsp(): Unit = {
    sendMsg("dsp", 67)
  }

  def dupscribe_and_dupscribe_detail(): Unit = {
    sendMsg("ssp_dupscribe", 67)
  }

  def image(): Unit = {
    sendMsg("image_show", 67)
  }

  def log(): Unit = {
    sendMsg("log", 11)
  }

  def offer(): Unit = {
    sendMsg("offer_fee", 67)
  }

  def topOffer(): Unit = {
    sendMsg("offer_click", 11)
  }

  def report_campaign(): Unit = {
    sendMsg("report_campaign_fill", 11)
  }

  def report_publisher(): Unit = {
    sendMsg("report_publisher_fill", 11)
  }


  def user_keep(): Unit = {
    sendMsg("user_keep", 18)
  }

  def user_na(): Unit = {
    sendMsg("user_new", 18)
  }

}

//object xxxx{
//  def main(args: Array[String]): Unit = {
//    val days = daysBetween("2017-12-18", "2017-12-18")
//
//    println(days + 1)
//    var hour = null.asInstanceOf[String]
//
//    (0 until days +1).map { x =>
//
//      (0 to 24).map{ y =>
//        hour = if(y < 10) s" 0$y:00:00" else s" $y:00:00"
//
//        println(DF.format(new Date(DF.parse("2017-12-18").getTime - x * 1000 * 60 * 60 * 24L)) + hour)
//      }
//    }
//  }
//}
