package com.mobikok.ssp.data.streaming.util.http;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.mobikok.ssp.data.streaming.util.ExecutorServiceUtil;
import okio.Okio;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Requests {
	
	private static Logger LOG = LoggerFactory.getLogger(Requests.class);

	private ExecutorService executorService;
	
	public Requests(int threadPoolSize){
		executorService = ExecutorServiceUtil.createdExecutorService(threadPoolSize);
	}

	public void get(String baseUrl){
		get(baseUrl, null);
	}

	public void get(String baseUrl, UrlParam urlParams){
		request("GET", baseUrl, urlParams, null, new Callback() {
					public void prepare(HttpURLConnection conn) {
					}

					public void completed(String responseStatus, String response) {
						LOG.warn("HTTP GET completed, responseStatus: " + responseStatus + " response: " + response);
					}

					public void failed(String responseStatus, String responseError, Exception ex) {
						LOG.warn("HTTP GET failed, responseStatus: " + responseStatus + " responseError: " + responseError + " exception: ", ex );
					}
				}, 10000, 10000, true);
	}

	public void get(String baseUrl, UrlParam urlParam, Callback callback){
		request("GET", baseUrl, urlParam, null, callback, 10000,10000,  true);
	}

	public void post(String baseUrl, Entity bodyParams){
		request("POST", baseUrl, null, bodyParams,  new Callback() {
			public void prepare(HttpURLConnection conn) {
			}

			public void completed(String responseStatus, String response) {
				LOG.warn("HTTP POST completed, responseStatus: " + responseStatus + " response: " + response);
			}

			public void failed(String responseStatus, String responseError, Exception ex) {
				LOG.warn("HTTP POST failed, responseStatus: " + responseStatus + " responseError: " + responseError + " exception: ", ex );
			}
		}, 10000,10000,  true);
	}

	public void post(String baseUrl, Entity bodyParams, Callback callback){
		request("POST", baseUrl, null, bodyParams, callback, 10000,10000,  true);
	}

    public void post(String baseUrl, Entity bodyParams, Callback callback, boolean isAsync) {
        request("POST", baseUrl, null, bodyParams, callback, Integer.MAX_VALUE, Integer.MAX_VALUE, isAsync);
    }

    public void post(String baseUrl, Entity bodyParams, int timeout, Callback callback, boolean isAsync) {
        request("POST", baseUrl, null, bodyParams, callback, timeout, timeout, isAsync);
    }

    public void put(String baseUrl, Entity bodyParams, Callback callback) {
        put(baseUrl, bodyParams, callback, true);
    }

	public void put(String baseUrl, Entity bodyParams, Callback callback, boolean isAsync){
		request("PUT", baseUrl, null, bodyParams, callback, 10000,10000, isAsync);
	}
	
	private void request(
			final String requestMethod,
			final String baseUrl,
			final UrlParam urlParams,
			final Entity bodyParams,
			final Callback callback,
			final int connectTimeout,
			final int socketTimeout,
			boolean isAsync){

		Runnable r = new Runnable() {

            public void run() {
                OutputStream out = null;
                InputStream in = null;
                HttpURLConnection conn = null;
                try {

					URL url = new URL(contcatParams(baseUrl, urlParams));
					conn = (HttpURLConnection) url.openConnection();
					conn.setReadTimeout(socketTimeout);
					conn.setConnectTimeout(connectTimeout);
					conn.setDoInput(true);
					conn.setRequestProperty("Connection", "Keep-Alive");
					if(bodyParams != null) {
						conn.setRequestProperty("Content-Type", bodyParams.contentType());
					}
					conn.setDoOutput(true);
					conn.setUseCaches(false);
					conn.setRequestMethod(requestMethod);
					callback.prepare(conn);
					conn.connect();

					if(bodyParams != null) {
						out = conn.getOutputStream();
						out.write(bodyParams.build().getBytes());
					}

					in = conn.getInputStream();

//					List<String> lines = IOUtils.readLines(in);
//					StringBuffer buff = new StringBuffer();
//					for(int i = 0; i <lines.size(); i++) {
//						buff.append(lines.get(i));
//					}
					String result = Okio.buffer(Okio.source(in)).readString(Charset.forName("UTF-8"));

					try{
						callback.completed(responseStatus(conn), result);
					}catch(Exception ex){
						LOG.error("调用Callback.completed方法报错: ", ex);
					}

				}catch(Exception e){
					//LOG.error("发送请求失败：", e);
					callback.failed(responseStatus(conn), responseError(conn), e);
				}finally {
					closeQuietly(out);
					closeQuietly(in);
				}
			}
		};

		if(isAsync){
			executorService.execute(r);
		}else {
			r.run();
		}
	}
	
	
	private static String responseError(HttpURLConnection conn){
		if(conn == null) return "";
		
		InputStream in = conn.getErrorStream();
		
		if(in == null) return "";

		StringBuffer buff = new StringBuffer();
		try {
			List<String> errors = IOUtils.readLines(in);
			
			for(String e : errors) {
				buff.append(e);
			}
			
		} catch (IOException ex) {
			LOG.error("获取响应的错误信息失败: ", ex);
		}
		return buff.toString();
		
	}
	
	private static String responseStatus(HttpURLConnection conn){
		if(conn == null) return "";
		try {
			return conn.getResponseCode() + " " + conn.getResponseMessage();
		} catch (IOException e) {
			return "";
		}
	}
	
	private static void closeQuietly(Closeable closeable) {
	        try {
	            if (closeable != null) {
	                closeable.close();
	            }
	        } catch (IOException ioe) {
	        	LOG.error("关闭流失败！", ioe);
	        }
	    }
	 
	private static String contcatParams(String baseUrl, UrlParam urlParams){
		if(urlParams == null || urlParams.isEmpty()) return baseUrl;
		
		if(baseUrl.contains("?")) {
			return baseUrl + "&" +urlParams.build();
		}
		return baseUrl + "?" + urlParams.build();
	}

	public void shutdown() {
		executorService.shutdown();
	}
	
}
