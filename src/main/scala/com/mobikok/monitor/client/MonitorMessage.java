package com.mobikok.monitor.client;

import java.util.Date;

public class MonitorMessage {

	public MonitorMessage(String topic, String date, Object value){
		this.topic = topic;
		this.date = date;
		this.value = value;
	}

	private String topic;
	private String date;
	private Object value;

	
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
}
