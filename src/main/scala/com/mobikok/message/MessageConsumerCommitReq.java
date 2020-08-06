package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

import java.util.Objects;

/**
 * Created by Administrator on 2017/8/17.
 */
public class MessageConsumerCommitReq extends JavaJSONSerializable {
    private String topic;
    private Long offset;
    private String consumer;

    /**
     * 局部提交，即，只提交该偏移的记录项，不包括该偏移之前的数据项偏移，
     * 当下次拉取数据时，只滤该偏移对应的项，若有小于该偏移的数据项，则依然可以拉取到
      */
    private boolean isPartialCommit = false;

//    public MessageConsumerCommitReq(){   }
    public MessageConsumerCommitReq(String consumer, String topic, Long offset){
       this(consumer, topic, offset, false);
    }
    public MessageConsumerCommitReq(String consumer, String topic, Long offset, boolean isPartialCommit){
        this.consumer = consumer;
        this.topic = topic;
        this.offset = offset;
        this.isPartialCommit = isPartialCommit;
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

    public boolean isPartialCommit() {
        return isPartialCommit;
    }

    public void setPartialCommit(boolean partialCommit) {
        isPartialCommit = partialCommit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageConsumerCommitReq that = (MessageConsumerCommitReq) o;
        return isPartialCommit == that.isPartialCommit &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(consumer, that.consumer);
    }

    @Override
    public int hashCode() {

        return Objects.hash(topic, consumer, isPartialCommit);
    }
}
