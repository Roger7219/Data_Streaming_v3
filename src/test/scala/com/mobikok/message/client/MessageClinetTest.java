package com.mobikok.message.client;

import com.mobikok.message.MessageConsumerCommitReq;
import com.mobikok.message.MessagePushReq;

/**
 * Created by Administrator on 2017/9/1.
 */
public class MessageClinetTest {

    public static void main(String[] args) {
//        http://node14:5555/Api/Message
//
//        {
//            "topic":"PublisherThirdIncome",
//            "key":"2013-12-12",
//            "uniqueKey":   true,
//            "data":""
//        }

//        push();
        commit();
    }

    public static void push(){
        MessageClientApi messageClinet = new MessageClientApi("","http://node14:5555/");


        messageClinet.pushMessage(new MessagePushReq(
                "compaction_ssp_offer_dwr",
                "2017-09-01 00:00:00",
                true,
                ""
        ));
    }

    public static void commit(){

        MessageClientApi messageClinet = new MessageClientApi("","http://localhost:5555/");

        //MessageConsumerCommitReq [topic=user_new, offset=20170918011211, consumer=GreenplumConsumer]
        //MessageConsumerCommitReq [topic=user_new, offset=20170918235921, consumer=GreenplumConsumer]
        messageClinet.commitMessageConsumer(new MessageConsumerCommitReq(
                "GreenplumConsumer",
                "user_new",
                20170918011211L
        ),new MessageConsumerCommitReq(
                "GreenplumConsumer",
                "user_new",
                20170918235921L
        ));

    }
}
