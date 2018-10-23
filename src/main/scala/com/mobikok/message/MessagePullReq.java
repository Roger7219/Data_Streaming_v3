package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

/**
 * Created by Administrator on 2017/8/17.
 */
public class MessagePullReq extends JavaJSONSerializable {
    //支持consumer全文匹配*
    private String consumer;
    private String[] topics;

//    public MessagePullReq(){ }

    public MessagePullReq(String consumer, String[] topics){
        this.consumer = consumer;
        this.topics = topics;
    }
    public String getConsumer() {
        return consumer;
    }
    public MessagePullReq setConsumer(String consumer) {
        this.consumer = consumer;
        return this;
    }
    public String[] getTopics() {
        return topics;
    }
    public MessagePullReq setTopics(String[] topics) {
        this.topics = topics;
        return this;
    }

}
