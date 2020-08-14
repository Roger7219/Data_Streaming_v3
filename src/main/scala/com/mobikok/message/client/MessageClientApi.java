package com.mobikok.message.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mobikok.message.*;
import com.mobikok.ssp.data.streaming.util.*;
import com.mobikok.ssp.data.streaming.util.http.Requests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by Administrator on 2017/8/17.
 */
public class MessageClientApi {
    private Logger LOG = null;
    private String apiBase;

    /**
     * @param apiBase eg: http://node14:5555
     */
    public MessageClientApi(String apiBase) {
        this(MessageClientApi.class.getSimpleName(), apiBase);
    }

    public MessageClientApi(String loggerName, String apiBase) {
        LOG = new Logger(loggerName, MessageClientApi.class, new Date().getTime());
        this.apiBase = apiBase;
    }

    //拉取 & offset升序
    public Resp<List<Message>> pullMessage(MessagePullReq req){

        return RunAgainIfError.runForJava(() -> {
            Resp<List<Message>> res = new Resp<List<Message>>();
            if(req.getTopics() == null || req.getTopics().length ==0  || StringUtil.isEmpty(req.getConsumer())) return res;

            StringBuffer bf = new StringBuffer();
            for (String t : req.getTopics()) {
                if (bf.length() > 0) {
                    bf.append("&");
                }
                try {
                    bf.append("topics=").append(URLEncoder.encode(t, "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            if(req.getSize() != null) {
                if(bf.length() > 0){
                    bf.append("&");
                }
                bf.append("size=").append(req.getSize());
            }

            String url = apiBase + "/Api/Message?consumer=" + req.getConsumer() + "&" + bf.toString();
            LOG.warnForJava("MessageClient pullMessage rest api request","url: " + url + ", req: " + OM.toJOSN(req));

            String resp = callRestApi(url, null, "GET");

            LOG.warnForJava("MessageClient pullMessage rest api response", resp);

            res = OM.toBean(resp, new TypeReference<Resp<List<Message>>>() {});

            //容错性处理，避免getPageData()为空时，后续操作抛空指针异常
            if(res.getPageData() == null) res.setPageData(Collections.EMPTY_LIST);

            //升序
            Collections.sort(res.getPageData(), new Comparator<Message>() {
                public int compare(Message a, Message b) {
                    return a.getOffset().compareTo(b.getOffset());
                }
            });

            if (res.getStatus() != 1) {
                throw new RuntimeException(res.getError());
            }
            return res;
        },"MessageClient pushMessage error",40);
    }

    //推x送
    public Resp<List<Message>> pushMessage(MessagePushReq... req){
        return pushMessage(false, req);
    }
    public Resp<List<Message>> pushMessage(boolean fastFail, MessagePushReq... req){
        Resp<List<Message>> res = new Resp<List<Message>>();
        if (req.length == 0) return res;

        boolean doCall = true;
        while (doCall) {
            try {
                StringBuffer bf = new StringBuffer();

                String url = apiBase + "/Api/Message";
                LOG.warnForJava("MessageClient pushMessage rest api request", "url: " + url + ", \nbody: " + OM.toJOSN(req));

                String resp = callRestApi(url, OM.toJOSN(req), "POST");

                LOG.warnForJava("MessageClient pushMessage rest api response", resp);

                res = OM.toBean(resp, new TypeReference<Resp<List<Message>>>() {
                });
                if (res.getStatus() != 1) {
                    throw new RuntimeException(res.getError());
                }else {
                    doCall = false;
                }
            }catch (Exception ex) {
                if(fastFail) {
                    throw new RuntimeException(ex);
                }else {
                    LOG.warnForJava("MessageClient pushMessage error, Will try again!! ", ex);
                    try {
                        Thread.sleep(1000*60);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return res;
    }

    //提交
    public RespOne<String> commitMessageConsumer(MessageConsumerCommitReq... req){
        return commitMessageConsumer(false, req);
    }
    public RespOne<String> commitMessageConsumer(boolean fastFail, MessageConsumerCommitReq... req){
        RespOne<String> res = new RespOne<String>();
        if(req.length == 0) return res;

        Arrays.sort(req, new Comparator<MessageConsumerCommitReq>() {
            public int compare(MessageConsumerCommitReq a, MessageConsumerCommitReq b) {
                return a.getOffset().compareTo(b.getOffset());
            }
        });

        boolean doCall = true;
        while (doCall) {
            try {
                StringBuffer bf = new StringBuffer();

                String url = apiBase +"/Api/Message/Consumer";
                LOG.warnForJava("MessageClient commitMessageConsumer rest api request", "url: " + url + ", \n body: " + OM.toJOSN(req));

                String resp = callRestApi(url, OM.toJOSN(req),"POST");

                LOG.warnForJava("MessageClient commitMessageConsumer rest api response", resp);

                res = OM.toBean(resp, new TypeReference<RespOne<String>>() {});

                if (res.getStatus() != 1) {
                    throw new RuntimeException(res.getError());
                }else {
                    doCall = false;
                }
            }catch (Exception ex) {
                if(fastFail) {
                    throw new RuntimeException(ex);
                }else {
                    LOG.warnForJava("MessageClient pushMessage error, Will try again!! ", ex);
                    try {
                        Thread.sleep(1000*60);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return res;
    }

    public static void main(String[] args) {

        MessageConsumerCommitReq[] s = {
                new MessageConsumerCommitReq("consumer", "topic", 112L),
                new MessageConsumerCommitReq("consumer", "topic", 13L)
        };

        Arrays.sort(s, new Comparator<MessageConsumerCommitReq>() {
            public int compare(MessageConsumerCommitReq a, MessageConsumerCommitReq b) {
                return a.getOffset().compareTo(b.getOffset());
            }
        });

        System.out.println(Arrays.deepToString(s) );


//        MessageClient messageClient = new MessageClient("http://localhost:5555/");
////        Object o = messageClient.pullMessage(new MessagePullReq().setConsumer("consumer1").setTopics(new String[]{"topoc1"}));
////        System.out.println("RES:\n"+ o);
//
////        messageClient.pushMessage(new MessagePushReq("topic2", "key2", true, "data2"));
//
//        messageClient.pullMessage(new MessagePullReq("consumer2", new String[]{"topic2"}));

        //messageClient.commitMessageConsumer(new MessageConsumerCommitReq("consomer2", "topic2", 1502966622889L));



    }

    private Requests requests = new Requests(30);

    private String callRestApi(String addr, String body, String requestMethod) {
        String result = "";
        try {
            URL url = new URL(addr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(20*1000);// 20秒
            connection.setReadTimeout(20*1000);   // 20秒
            connection.setRequestMethod(requestMethod/*"PUT"*/);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type",
                    "application/json;charset=UTF-8");
            if (body != null) {
                PrintWriter out = new PrintWriter(connection.getOutputStream());
                out.write(body);
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
            throw new RuntimeException("REST API request fail: ", e);
        }
        return result;
    }

    public String getApiBase() {
        return apiBase;
    }

}
