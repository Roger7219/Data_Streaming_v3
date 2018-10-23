package com.mobikok.ssp.data.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.mobikok.message.{MessageConsumerCommitReq, MessagePullReq, MessagePushReq}
import com.mobikok.message.client.MessageClient
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart
import com.mobikok.ssp.data.streaming.util.OM

/**
  * Created by Administrator on 2017/9/26.
  */
object GreenplumRefreshUtil {

  val DF = new SimpleDateFormat("yyyy-MM-dd")
  val messageClient = new MessageClient("", "http://node14:5555")

  def main (args: Array[String]): Unit = {
    campaign
//    resetPublisher()
//    publisher22
  }


  def campaign (): Unit = {

    //report_campaign_fill
    var s = (0 until 30).map { x =>

      new MessagePushReq(
        "report_campaign_fill",
          OM.toJOSN(Array(Array(HivePartitionPart("b_date", DF.format(new Date(new Date().getTime - x * 1000 * 60 * 60 * 24L)))))),
          true,
          ""
        )
    }

    println(s)

    messageClient.pushMessage(s: _*);

  }

  def resetPublisher(){

    val req= new MessagePullReq("GreenplumConsumer", Array(
      "PublisherThirdIncomeForGreenplumReflush"
      ,"report_publisher_user_new"
      ,"report_publisher_user_active"
      ,"report_publisher_fee"
      ,"report_publisher_click"
      ,"report_publisher_show"
      ,"report_publisher_send"
      ,"report_publisher_fill"
    ))

    import scala.collection.JavaConversions._
    val xx=messageClient.pullMessage(req)
    val c=xx.getPageData.map{x=>
      new MessageConsumerCommitReq("GreenplumConsumer", x.getTopic,x.getOffset)
    }

    messageClient.commitMessageConsumer(c:_*)
  }

  def publisher22(): Unit ={

    val ss=Array(
      "2017-10-12",
      "2017-10-13"
      ).map{x=>
       new MessagePushReq(
        "report_publisher_user_active",
        OM.toJOSN(Array(Array(HivePartitionPart("b_date", x)))),
        true,
        ""
      )
    }

    println(ss)

    messageClient.pushMessage(ss:_*);
  }

  def publisher2 (): Unit = {

    //report_campaign_fill
    var s = (1 until 29).map { x =>

      new MessagePushReq(
        "report_publisher_user_active",
        OM.toJOSN(Array(Array(HivePartitionPart("b_date", DF.format(new Date(new Date().getTime - x * 1000 * 60 * 60 * 24L)))))),
        true,
        ""
      )
    }

    println(s)

    messageClient.pushMessage(s: _*);

  }


  def publisher (): Unit = {
    var s = (1 until 10).map { x =>
      new MessagePushReq(
        "PublisherThirdIncome",
        s"2017-08-0$x",
        true,
        ""
      )
    }.union(
      (10 until 32).map { x =>
        new MessagePushReq(
          "PublisherThirdIncome",
          s"2017-09-$x",
          true,
          ""
        )
      }
    )
      .union(
        (1 until 10).map { x =>
          new MessagePushReq(
            "PublisherThirdIncome",
            s"2017-09-0$x",
            true,
            ""
          )
        }
      )
      .union(
        (10 until 31).map { x =>
          new MessagePushReq(
            "PublisherThirdIncome",
            s"2017-09-$x",
            true,
            ""
          )
        }
      )
    //    s = (1 until 2).map{x=>
    //      new MessagePushReq(
    //        "PublisherThirdIncome",  config_update
    //        s"2017-08-0$x",      tablename 1
    //        true,
    //        ""
    //      )
    //    }

    println(s)
    messageClient.pushMessage(s: _*);
  }
}
