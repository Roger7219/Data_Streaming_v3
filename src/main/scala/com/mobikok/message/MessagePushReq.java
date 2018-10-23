package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

/**
 * Created by Administrator on 2017/8/17.
 */
public class MessagePushReq extends JavaJSONSerializable {
    private String topic;
    private String key;
    /**
     * 当uniqueKey设置为true,且key重复，会覆盖之前的记录（如果之前存在）最终只有一条记录
     */
    private boolean uniqueKey = true;
    private String data;


//    public MessagePushReq() {
//        super();
//    }

    public MessagePushReq(String topic, String key){
        this(topic, key,true, "");
    }

    public MessagePushReq(String topic, String key, boolean uniqueKey, String data){
        this.topic = topic;
        this.key = key;
        this.uniqueKey = uniqueKey;
        this.data = data;
    }

    public boolean isUniqueKey() {
        return uniqueKey;
    }
    public void setUniqueKey(boolean uniqueKey) {
        this.uniqueKey = uniqueKey;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
}
