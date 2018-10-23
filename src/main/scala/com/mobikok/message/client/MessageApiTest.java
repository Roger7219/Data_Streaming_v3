package com.mobikok.message.client;
import com.mobikok.ssp.data.streaming.util.OM;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * Created by Administrator on 2017/8/17.
 */
public class MessageApiTest {

    public static void main(String[] args) {

        //推送
//		List push = new ArrayList<HashMap>() {{
//			HashMap pushReq = new HashMap<String, Object>(){{
//				put("topic", "topic1");
//				put("key", "key1");
//				put("uniqueKey", true);
//				put("data", "data1_2");
//			}};
//			store(pushReq);
//		}};
//
//		System.out.println("推送：\n" + callRestApi("http://localhost:5555/Api/Message",OM.toJOSN(push),"POST"));

        //拉取
        //System.out.println("拉取：\n" + callRestApi("http://node14:5555/Api/Message?consumer=consumer1&topics=app_show", null,"GET"));

        //
		List commit = new ArrayList<HashMap>(){{
			HashMap commitReq = new HashMap<String, Object>(){{
				put("consumer", "consumer1");
				put("topic", "topic1");
				put("offset", 1502955503119L);
			}};
			add(commitReq);
		}};
//
		//提交
		System.out.println("提交：\n" + callRestApi("http://104.250.136.138:5555/Api/Message/Consumer",
				OM.toJOSN(commit),"POST"));

    }

    private static String callRestApi(String addr, String params, String requestMethod) {
        System.out.println(params);
        String result = "";
        try {
            URL url = new URL(addr);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod(requestMethod/*"PUT"*/);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type",
                    "application/json;charset=UTF-8");
            if(params != null) {
                PrintWriter out = new PrintWriter(connection.getOutputStream());
                out.write(params);
                out.close();
            }
            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(
                        connection.getInputStream()));
            } catch (Exception exception) {
                java.io.InputStream err = ((HttpURLConnection) connection)
                        .getErrorStream();
                if (err == null)
                    throw exception;
                in = new BufferedReader(new InputStreamReader(err));
            }
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null)
                response.append(line + "\n");
            in.close();

            result = response.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
