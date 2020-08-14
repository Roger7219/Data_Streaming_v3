package com.mobikok.monitor.client;

import com.mobikok.message.client.MessageClientApi;
import com.mobikok.ssp.data.streaming.util.JavaMessageClient;
import com.mobikok.ssp.data.streaming.util.PushReq;

public class MonitorClient {

	private MessageClientApi messageClient;//= new MessageClient("http://node14:5555");

	public MonitorClient(MessageClientApi messageClientApi){
		this.messageClient = messageClientApi;
	}

	public void push(MonitorMessage message ){

		String key = message.getDate() + "^" + String.valueOf(message.getValue());
		JavaMessageClient.push(this.messageClient, PushReq.apply(message.getTopic(), key));
	}

}
