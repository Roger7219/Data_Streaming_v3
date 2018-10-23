package com.mobikok.ssp.data.streaming.util.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.ParseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class Form extends Entity {
	private List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();

	protected Form(){ }
	
	public Form add(String name, String value){
		params.add(new BasicNameValuePair(name, value));
		return this;
	}
	public Form addParam(String name, Integer value){
		params.add(new BasicNameValuePair(name, String.valueOf(value)));
		return this;
	}
	public String build() {
		try {
			return EntityUtils.toString(new UrlEncodedFormEntity(params, "UTF-8"));
		} catch (Exception e) {
			throw new RuntimeException("Form data convert to string fail!", e);
		}
	}

	
}
