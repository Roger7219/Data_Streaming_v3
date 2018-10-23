package com.mobikok.message;


import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable;
import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

import java.util.Date;

/*

create table message(
	topic varchar(200),
	keyBody   varchar(200),
	keySalt varchar(200),
	data varchar(2000),
	offset bigint(20),
	updateTime timestamp default current_timestamp on update current_timestamp,
	primary key(topic, keyBody, keySalt)
)ENGINE=InnoDB DEFAULT CHARSET=utf8


create table message_consumer(
	consumer varchar(200),
	topic varchar(200),
	offset bigint(20),
	updateTime timestamp default current_timestamp on update current_timestamp,
	primary key(consumer, topic)
)engine=InnoDB DEFAULT CHARSET=utf8


*/

public class Message extends JavaJSONSerializable {

    //若uniqueKey为true，则联合主键: topic、key和keySalt
    private String topic;
    private String keyBody = null;
    private String keySalt = "-";

    private String data;

    //保存时生产
    private Long offset;
    private Date updateTime;



    public Message() {
        super();
    }
    public Message(String topic, String keyBody, String keySalt, String data,
                   Long offest) {
        super();
        this.topic = topic;
        this.keyBody = keyBody;
        this.keySalt = keySalt;
        this.data = data;
        this.offset = offest;
    }
    public Long getOffset() {
        return offset;
    }
    public void setOffset(Long offest) {
        this.offset = offest;
    }

    public String getKeySalt() {
        return keySalt;
    }
    public void setKeySalt(String keySalt) {
        this.keySalt = keySalt;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getKeyBody() {
        return keyBody;
    }
    public void setKeyBody(String keyBody) {
        this.keyBody = keyBody;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    public Date getUpdateTime() {
        return updateTime;
    }
    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

}
