package com.mobikok.ssp.data.streaming.util.http;

import java.util.List;

import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Str extends Entity {
	
	protected Str(){}
	
	private String str;
	private String contentType;

	public Str(String str, String contentType) {
		this.str = str;
		this.contentType = contentType;
	}

	public String build() {
		return str;
	}
	
	public String toString() {
		return build();
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
}
