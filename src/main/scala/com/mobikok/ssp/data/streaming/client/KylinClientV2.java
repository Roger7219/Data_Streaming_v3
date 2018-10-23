package com.mobikok.ssp.data.streaming.client;

import com.mobikok.ssp.data.streaming.util.http.Callback;
import com.mobikok.ssp.data.streaming.util.http.Entity;
import com.mobikok.ssp.data.streaming.util.http.Requests;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.net.HttpURLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017/7/12.
 */
public class KylinClientV2 {

    private org.slf4j.Logger LOG = LoggerFactory.getLogger(getClass());
    private String account = "ADMIN";
    private String password = "KYLIN";
    private String requestAuthorizationCode;
    private String kylinApiUrl = "http://node14:7070/kylin/api/";

    private String ERROR_REFRESH_NOT_EXISTING_SEGMENT = "does not match any existing segment in cube CUBE";
    //同时刷新一个Cube报的异常
    private final static String ERROR_SEGMENTS_OVERLAP = "Segments overlap";

    private Requests requests = new Requests(30);

    private long TZ_OFFSET = 8 * 60 * 60 * 1000;

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public KylinClientV2(String kylinApiUrl) {
       this(kylinApiUrl, null, null);
    }

    public KylinClientV2(String kylinApiUrl,
                         String kylinAccount,
                         String kylinPassword) {
        this.kylinApiUrl = kylinApiUrl;
        this.account = kylinAccount == null ? "ADMIN" : kylinAccount;
        this.password = kylinPassword == null ? "KYLIN" : kylinPassword;
        String auth = account + ":" + password;
        String code = new String(new Base64().encode(auth.getBytes()));
        this.requestAuthorizationCode = "Basic " + code;
    }


    public void refreshOrBuildCube(String cubeName, String startTime,String endTime) {
        refreshOrBuildCube(cubeName, startTime, endTime, new Callback() {
            public void prepare(HttpURLConnection conn) {}
            public void completed(String responseStatus, String response) {}
            public void failed(String responseStatus, String responseError, Exception ex) {}
        },true);
    }

    public void refreshOrBuildCube(String cubeName, String startTime,String endTime, Callback callback, final boolean isAsync) {

        try {
            Date s = simpleDateFormat.parse(startTime);
            Date e = simpleDateFormat.parse(endTime);
            refreshOrBuildCube(cubeName, s, e, callback, isAsync);

        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }

    }

    public void refreshOrBuildCube(final String cubeName, final Date startTime, final Date endTime, final Callback callback) {
        refreshOrBuildCube(cubeName, startTime, endTime, callback, true);
    }

    public void refreshOrBuildCube(final String cubeName, final Date startTime, final Date endTime, final Callback callback, final boolean isAsync) {
        refreshCube(cubeName, startTime, endTime, new Callback() {
            public void prepare(HttpURLConnection conn) {
                callback.prepare(conn);
            }

            public void completed(String responseStatus, String response) {
                callback.completed(responseStatus, response);
            }

            public void failed(String responseStatus, String responseError, Exception ex) {
                //LOG.warn("Refresh Cube Fail !!",ex);
                if (responseError.contains(ERROR_REFRESH_NOT_EXISTING_SEGMENT)) {
                    LOG.warn("Refresh not existing segment, Will call build segment API");
                    rebuildCube(cubeName, startTime, endTime, new Callback() {
                        public void prepare(HttpURLConnection conn) {
                            callback.prepare(conn);
                        }

                        public void completed(String responseStatus, String response) {
                            callback.completed(responseStatus, response);
                        }

                        public void failed(String responseStatus, String responseError, Exception ex) {
                            //LOG.warn("Rebuild Cube Fail !!",ex);
                            callback.failed(responseStatus, responseError, ex);
                        }
                    }, isAsync);
                } else {
                    callback.failed(responseStatus, responseError, ex);
                }
            }

        }, isAsync);
    }
    public void refreshCube(String cubeName, Date startTime, Date endTime, Callback callback, boolean isAsync) {
        callBuildCubeApi(cubeName, startTime, endTime, "REFRESH", callback, isAsync);
    }

    public void rebuildCube(String cubeName, Date startTime, Date endTime, Callback callback,  boolean isAsync) {
        callBuildCubeApi(cubeName, startTime, endTime, "BUILD", callback, isAsync);
    }

    private void callBuildCubeApi(final String cubeName, final Date startTime, final Date endTime, final String buildType, final Callback callback, boolean isAsync) {
        final String url = kylinApiUrl + "/cubes/" + cubeName + "/rebuild"; //"/cubes/cube_ssp_traffic_overall_dm_final/rebuild";

        final Entity params = new Entity().setJson(new HashMap<String, Object>() {{
            put("startTime", startTime.getTime() + TZ_OFFSET);
            put("endTime", endTime.getTime() + TZ_OFFSET);
            put("buildType", buildType);
        }});
        requests.put(
                url,
                params,
                new Callback() {
                    public void prepare(HttpURLConnection conn) {
                        conn.setRequestProperty("Authorization", requestAuthorizationCode);
                        callback.prepare(conn);
                    }

                    public void completed(String responseStatus, String response) {
                        log(url, params.build(), responseStatus, response);
                        callback.completed(responseStatus, response);
                    }

                    public void failed(String responseStatus, String responseError, Exception ex) {
                        log(url, params.build(), responseStatus, responseError);
                        callback.failed(responseStatus, responseError, ex);
                    }
                },
                isAsync
        );
    }

    private void log(String url, String requestBody, String responseStatus, String resp) {
        StringBuffer log = new StringBuffer();
        log.append("\n---------------------------------------↓↓↓---------------------------------------")
                .append("\nRequest  uri    : PUT {}")
                .append("\nRequest  body   : {}")
                .append("\nResponse status : {}")
                .append("\nResponse body   : {}");
        log.append("\n---------------------------------------↑↑↑---------------------------------------");

        LOG.info(log.toString(),
                new String[]{
                        url,
                        requestBody,
                        responseStatus,
                        resp});
    }



    public static void main(String[] args) throws InterruptedException {
        KylinClientV2 kylinClient = new KylinClientV2(
                "http://node14:7070/kylin/api",
                "ADMIN",
                "KYLIN");
//        kylinClient.rebuildCube("cube_ssp_traffic_overall_dm_final", new Date(1451750400000L), new Date(1451836800000L));

        kylinClient.refreshOrBuildCube(
                "cube_dsp_traffic_dm",
                "2012-09-09 22:22:22",
                "2012-09-10 22:22:22");

        Thread.sleep(1000*5);
    }

}


//    refreshOrBuildCube(cubeName, startTime, endTime, new Callback() {
//            public void prepare(HttpURLConnection conn) {
//            }
//
//            public void completed(String responseStatus, String response) {
//
//            }
//
//            public void failed(String responseStatus, String responseError, Exception ex) {
//
//                dispatcherTask();
//            }
//        });


//        final String url = kylinApiUrl + "/cubes/" + cubeName + "/rebuild"; //"/cubes/cube_ssp_traffic_overall_dm_final/rebuild";
//
//        final Entity params = new Entity().setJson(new HashMap<String, Object>() {{
//            put("startTime", startTime.getTime() + TZ_OFFSET);
//            put("endTime", endTime.getTime() + TZ_OFFSET);
//            put("buildType", "BUILD");
//        }});
//        requests.put(
//                url,
//                params,
//                new Callback() {
//                    public void prepare(HttpURLConnection conn) {
//                        conn.setRequestProperty("Authorization", requestAuthorizationCode);
//                    }
//
//                    public void completed(String responseStatus, String response) {
//                        log(url, params.build(), responseStatus, response);
//                    }
//
//                    public void failed(String responseStatus, String responseError, Exception ex) {
//                        log(url, params.build(), responseStatus, responseError);
//                    }
//                });
