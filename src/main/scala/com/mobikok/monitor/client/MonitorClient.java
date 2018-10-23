package com.mobikok.monitor.client;

import com.mobikok.message.client.MessageClient;
import com.mobikok.ssp.data.streaming.util.MessageClientUtil;
import com.mobikok.ssp.data.streaming.util.PushReq;

public class MonitorClient {

	MessageClient messageClient;//= new MessageClient("http://node14:5555");

	public MonitorClient(MessageClient messageClient){
		this.messageClient = messageClient;
	}

	public void push(MonitorMessage message ){

		String key = message.getDate() + "^" + String.valueOf(message.getValue());
		MessageClientUtil.push(this.messageClient, PushReq.apply(message.getTopic(), key));
	}

}
