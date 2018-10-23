//package com.mobikok.ssp.data.streaming.util;
//
//import com.fasterxml.jackson.annotation.JsonInclude;
//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.core.json.PackageVersion;
//import com.fasterxml.jackson.databind.JsonSerializer;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.SerializerProvider;
//import com.fasterxml.jackson.databind.module.SimpleModule;
//import com.mobikok.ssp.data.streaming.exception.JSONEntitySerializationException;
//import org.apache.commons.io.IOUtils;
//import org.apache.http.client.entity.UrlEncodedFormEntity;
//import org.apache.http.message.BasicNameValuePair;
//import org.apache.http.util.EntityUtils;
//import org.slf4j.*;
//
//import java.io.Closeable;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//
///**
// * Created by Administrator on 2018/3/6.
// */
//public class HTTP {
//
//
//
//    public interface Callback {
//
//        void prepare(HttpURLConnection conn);
//        void completed(String responseStatus, String response);
//        void failed(String responseStatus, String responseError, Exception ex);
//    }
//
//    public static class Entity {
//
//        public static String CONTENT_TYPE_JSON = "application/json";
//        public static String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";
//
//        public static String CONTENT_TYPE_TEXT = "text/html";
//
//        private Str str;
//        private JSON json;
//        private Form form;
//
//        private Entity.Type currType;
//
//        public Entity setStr(String str, String contentType){
//            checkType(Entity.Type.STR);
//            this.currType = Entity.Type.STR;
//            this.str =  new Str(str, contentType);
//            return this;
//        }
//
//        public Entity setJson(Object bean){
//            checkType(Entity.Type.JSON);
//            this.currType = Entity.Type.JSON;
//            json = new JSON(bean);
//
//            return this;
//        }
//        public Entity addForm(String name, String value){
//            checkType(Entity.Type.FORM);
//            this.currType = Entity.Type.FORM;
//            if(form == null) {
//                form = new Form();
//            }
//            form.add(name, value);
//            return this;
//        }
//
//        public String build(){
//            switch (getCurrType()) {
//                case STR:
//                    return str.build();
//                case FORM:
//                    return form.build();
//                case JSON:
//                    return json.build();
//                default:
//                    return "";
//            }
//        }
//        private Entity.Type getCurrType() {
//            return currType;
//        }
//
//        public String contentType(){
//            switch (getCurrType()) {
//                case STR:
//                    return str.getContentType();
//                case FORM:
//                    return CONTENT_TYPE_FORM;
//                case JSON:
//                    return CONTENT_TYPE_JSON;
//                default:
//                    return CONTENT_TYPE_TEXT;
//            }
//        }
//
//        private void checkType(Entity.Type type){
//            if(currType != type && currType != null) {
//                throw new RuntimeException("Entity content type already is " + currType +", Cannot reset another type data");
//            }
//        }
//        public enum Type{
//            STR,JSON,FORM
//        }
//    }
//
//    public static class Form extends Entity {
//        private List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
//
//        protected Form(){ }
//
//        public Form add(String name, String value){
//            params.add(new BasicNameValuePair(name, value));
//            return this;
//        }
//        public Form addParam(String name, Integer value){
//            params.add(new BasicNameValuePair(name, String.valueOf(value)));
//            return this;
//        }
//        public String build() {
//            try {
//                return EntityUtils.toString(new UrlEncodedFormEntity(params, "UTF-8"));
//            } catch (Exception e) {
//                throw new RuntimeException("Form data convert to string fail!", e);
//            }
//        }
//
//
//    }
//
//
//    public static class JSON extends Entity {
//
//        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
//                .registerModule(new SimpleModule("DateDeserializer", PackageVersion.VERSION)
//                        .addSerializer(Date.class, new JsonSerializer<Date>() {
//                            @Override
//                            public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
//
//                                SimpleDateFormat formatter = CSTTime.formatter("yyyy-MM-dd HH:mm:ss");//new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                                String formattedDate = formatter.format(value);
//                                gen.writeString(formattedDate);
//                            }
//                        })
//                )
//                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
//                .setDateFormat(CSTTime.formatter("yyyy-MM-dd HH:mm:ss") /*new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")*/);;
//
//        protected JSON(){ }
//
//        private Object bean;
//
//        public JSON(Object bean) {
//            this.bean = bean;
//        }
//
//        public String build() {
//            try {
//                return OBJECT_MAPPER.writeValueAsString(bean);
//            } catch (JsonProcessingException e) {
//                throw new JSONEntitySerializationException(e);
//            }
//        }
//
//        public String toString() {
//            return build();
//        }
//    }
//
//    public static class JSONEntitySerializationException extends RuntimeException{
//        public JSONEntitySerializationException(Exception e) {
//            super(e);
//        }
//    }
//
//    public static class Requests {
//
//        private static org.slf4j.Logger LOG = LoggerFactory.getLogger(Requests.class);
//
//        private ExecutorService executorService;
//
//        public Requests(int threadPoolSize){
//            executorService = ExecutorServiceUtil.createdExecutorService(threadPoolSize);
//        }
//
//        public void get(String baseUrl){
//            get(baseUrl, null);
//        }
//
//        public void get(String baseUrl, UrlParam urlParams){
//            request("GET", baseUrl, urlParams, null, new Callback() {
//                        public void prepare(HttpURLConnection conn) {
//                        }
//
//                        public void completed(String responseStatus, String response) {
//                            LOG.warn("HTTP GET completed, responseStatus: " + responseStatus + " response: " + response);
//                        }
//
//                        public void failed(String responseStatus, String responseError, Exception ex) {
//                            LOG.warn("HTTP GET failed, responseStatus: " + responseStatus + " responseError: " + responseError + " exception: ", ex );
//                        }
//                    }
//                    , 10000, 10000, true);
//        }
//
//        public void get(String baseUrl, UrlParam urlParam, Callback callback){
//            request("GET", baseUrl, urlParam, null, callback, 10000,10000,  true);
//        }
//
//        public void post(String baseUrl, Entity bodyParams){
//            request("POST", baseUrl, null, bodyParams,  new Callback() {
//                public void prepare(HttpURLConnection conn) {
//                }
//
//                public void completed(String responseStatus, String response) {
//                    LOG.warn("HTTP POST completed, responseStatus: " + responseStatus + " response: " + response);
//                }
//
//                public void failed(String responseStatus, String responseError, Exception ex) {
//                    LOG.warn("HTTP POST failed, responseStatus: " + responseStatus + " responseError: " + responseError + " exception: ", ex );
//                }
//            }, 10000,10000,  true);
//        }
//
//        public void post(String baseUrl, Entity bodyParams, Callback callback){
//            request("POST", baseUrl, null, bodyParams, callback, 10000,10000,  true);
//        }
//
//        public void put(String baseUrl, Entity bodyParams, Callback callback){
//            put(baseUrl, bodyParams, callback, true);
//        }
//
//        public void put(String baseUrl, Entity bodyParams, Callback callback, boolean isAsync){
//            request("PUT", baseUrl, null, bodyParams, callback, 10000,10000, isAsync);
//        }
//
//        private void request(
//                final String requestMethod,
//                final String baseUrl,
//                final UrlParam urlParams,
//                final Entity bodyParams,
//                final Callback callback,
//                final int connectTimeout,
//                final int socketTimeout,
//                boolean isAsync){
//
//            Runnable r = new Runnable() {
//
//                public void run() {
//
//                    OutputStream out = null;
//                    InputStream in = null;
//                    HttpURLConnection conn = null;
//                    try {
//
//                        URL url = new URL(contcatParams(baseUrl, urlParams));
//                        conn = (HttpURLConnection) url.openConnection();
//                        conn.setReadTimeout(socketTimeout);
//                        conn.setConnectTimeout(connectTimeout);
//                        conn.setDoInput(true);
//                        conn.setRequestProperty("Connection", "Keep-Alive");
//                        if(bodyParams != null) {
//                            conn.setRequestProperty("Content-Type", bodyParams.contentType());
//                        }
//                        conn.setDoOutput(true);
//                        conn.setUseCaches(false);
//                        conn.setRequestMethod(requestMethod);
//                        callback.prepare(conn);
//                        conn.connect();
//
//                        if(bodyParams != null) {
//                            out = conn.getOutputStream();
//                            out.write(bodyParams.build().getBytes());
//                        }
//
//                        in = conn.getInputStream();
//
//                        List<String> lines = IOUtils.readLines(in);
//                        StringBuffer buff = new StringBuffer();
//                        for(int i = 0; i <lines.size(); i++) {
//                            buff.append(lines.get(i));
//                        }
//
//                        try{
//                            callback.completed(responseStatus(conn), buff.toString());
//                        }catch(Exception ex){
//                            LOG.error("调用Callback.completed方法报错: ", ex);
//                        }
//
//                    }catch(Exception e){
//                        //LOG.error("发送请求失败：", e);
//                        callback.failed(responseStatus(conn), responseError(conn), e);
//                    }finally {
//                        closeQuietly(out);
//                        closeQuietly(in);
//                    }
//                }
//            };
//
//            if(isAsync){
//                executorService.execute(r);
//            }else {
//                r.run();
//            }
//        }
//
//
//        private static String responseError(HttpURLConnection conn){
//            if(conn == null) return "";
//
//            InputStream in = conn.getErrorStream();
//
//            if(in == null) return "";
//
//            StringBuffer buff = new StringBuffer();
//            try {
//                List<String> errors = IOUtils.readLines(in);
//
//                for(String e : errors) {
//                    buff.append(e);
//                }
//
//            } catch (IOException ex) {
//                LOG.error("获取响应的错误信息失败: ", ex);
//            }
//            return buff.toString();
//
//        }
//
//        private static String responseStatus(HttpURLConnection conn){
//            if(conn == null) return "";
//            try {
//                return conn.getResponseCode() + " " + conn.getResponseMessage();
//            } catch (IOException e) {
//                return "";
//            }
//        }
//
//        private static void closeQuietly(Closeable closeable) {
//            try {
//                if (closeable != null) {
//                    closeable.close();
//                }
//            } catch (IOException ioe) {
//                LOG.error("关闭流失败！", ioe);
//            }
//        }
//
//        private static String contcatParams(String baseUrl, UrlParam urlParams){
//            if(urlParams == null || urlParams.isEmpty()) return baseUrl;
//
//            if(baseUrl.contains("?")) {
//                return baseUrl + "&" +urlParams.build();
//            }
//            return baseUrl + "?" + urlParams.build();
//        }
//
//        public void shutdown() {
//            executorService.shutdown();
//        }
//
//    }
//
//    public static class Str extends Entity {
//
//        protected Str(){}
//
//        private String str;
//        private String contentType;
//
//        public Str(String str, String contentType) {
//            this.str = str;
//            this.contentType = contentType;
//        }
//
//        public String build() {
//            return str;
//        }
//
//        public String toString() {
//            return build();
//        }
//
//        public String getContentType() {
//            return contentType;
//        }
//
//        public void setContentType(String contentType) {
//            this.contentType = contentType;
//        }
//    }
//
//    public static class UrlParam {
//
//        private Form form;
//
//        private UrlParam.Type currType;
//
//        public UrlParam addParam(String name, String value){
//            checkType(HTTP.UrlParam.Type.FORM);
//            this.currType = UrlParam.Type.FORM;
//            if(form == null) {
//                form = new Form();
//            }
//            form.add(name, value);
//            return this;
//        }
//
//        public String build(){
//            switch (getCurrType()) {
//                case FORM:
//                    return form.build();
//                default:
//                    return "";
//            }
//        }
//        public UrlParam.Type getCurrType() {
//            return currType;
//        }
//
//        private void checkType(UrlParam.Type type){
//            if(currType != type && currType != null) {
//                throw new RuntimeException("Entity content type already is " + currType +", Cannot reset another type data");
//            }
//        }
//        enum Type{
//            FORM
//        }
//        public boolean isEmpty() {
//            return form == null;
//        }
//    }
//
//}
//
//
