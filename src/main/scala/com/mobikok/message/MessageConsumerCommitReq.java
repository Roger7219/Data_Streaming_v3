package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

/**
 * Created by Administrator on 2017/8/17.
 */
public class MessageConsumerCommitReq extends JavaJSONSerializable {
    private String topic;
    private Long offset;
    private String consumer;
//    public MessageConsumerCommitReq(){   }
    public MessageConsumerCommitReq(String consumer, String topic, Long offset){
        this.consumer = consumer;
        this.topic = topic;
        this.offset = offset;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public Long getOffset() {
        return offset;
    }
    public void setOffset(Long offset) {
        this.offset = offset;
    }
    public String getConsumer() {
        return consumer;
    }
    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

}
