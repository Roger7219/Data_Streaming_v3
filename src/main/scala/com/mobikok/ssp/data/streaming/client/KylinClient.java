package com.mobikok.ssp.data.streaming.client;

import com.mobikok.ssp.data.streaming.util.Command;
import com.mobikok.ssp.data.streaming.util.MySqlJDBCClient;
import com.mobikok.ssp.data.streaming.util.http.Callback;
import com.mobikok.ssp.data.streaming.util.http.Entity;
import com.mobikok.ssp.data.streaming.util.http.Requests;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017/7/12.
 */
@Deprecated
public class KylinClient {

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

    private volatile boolean isStopTask = false;
    private Object lock = new Object();

    public KylinClient(String kylinApiUrl) {
       this(null, null, kylinApiUrl);
    }

    public KylinClient(String kylinApiUrl,
                       String kylinAccount,
                       String kylinPassword) {
        this.kylinApiUrl = kylinApiUrl;
        this.account = kylinAccount == null ? "ADMIN" : "ADMIN";
        this.password = kylinPassword == null ? "KYLIN" : "KYLIN";
        String auth = account + ":" + password;
        String code = new String(new Base64().encode(auth.getBytes()));
        this.requestAuthorizationCode = "Basic " + code;
        dispatcherTaskThread.start();
    }


    public void refreshOrBuildCube(final String cubeName, final Date startTime, final Date endTime, final Callback callback) {

        refreshCube(cubeName, startTime, endTime, new Callback() {
            public void prepare(HttpURLConnection conn) {
                callback.prepare(conn);
            }

            public void completed(String responseStatus, String response) {
                callback.completed(responseStatus, response);
            }

            public void failed(String responseStatus, String responseError, Exception ex) {
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
                            callback.failed(responseStatus, responseError, ex);
                        }
                    });
                } else {
                    callback.failed(responseStatus, responseError, ex);
                }
            }

        });
    }

    private Set<CubeTask> cubeTasks = new HashSet<CubeTask>();

    public void refreshOrBuildCubeAtLeastOnceTask(final List<String> cubeNames, final Date day) {
        try{
            Calendar c = Calendar.getInstance();
            c.setTime(day);

            c.set(Calendar.HOUR,0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND, 0);
            Date st = c.getTime();
            c.add(Calendar.DATE, 1);
            Date et = c.getTime();

            if(cubeNames != null) {
                for(String cube : cubeNames){
                    refreshOrBuildCubeAtLeastOnceTask(cube, st, et);
                }
            }
        }catch (Exception e){
            LOG.warn("kylinClient updateKylinCube fail", e);
        }
    }

    public void refreshOrBuildCubeAtLeastOnceTask(final String cubeName, final Date startTime, final Date endTime) {
        CubeTask cubeTask = new CubeTask(cubeName, startTime, endTime);
        cubeTasks.add(cubeTask);

        dispatcherTask();
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void refreshOrBuildCubeAtLeastOnceTask(final String cubeName, final String startTime, final String endTime) {
        try {
            Date _startTime = simpleDateFormat.parse(startTime);
            Date _endTime = simpleDateFormat.parse(endTime);

            refreshOrBuildCubeAtLeastOnceTask(cubeName, _startTime, _endTime);
        } catch (ParseException e) {
            throw new RuntimeException("解析日期异常：", e);
        }
    }

    private Thread dispatcherTaskThread = new Thread(new Runnable() {
        public void run() {
            while (true) {
                if(isStopTask) return;

                for (final CubeTask t : cubeTasks) {
                    cubeTasks.remove(t);
                    refreshOrBuildCube(t.cubeName, t.startTime, t.endTime, new Callback() {
                        public void prepare(HttpURLConnection conn) {
                        }

                        public void completed(String responseStatus, String response) {
                        }

                        public void failed(String responseStatus, String responseError, Exception ex) {
                            cubeTasks.add(t);
                            LOG.error("Refresh or build cube fail, Task will try again!");
                        }
                    });
                }

                try {
                    synchronized (lock) {
                        lock.wait(1000 * 60);
                        LOG.warn("Dispatcher task thread stoped waiting");
                    }
                } catch (Exception e) {
                    LOG.error("Cube dispatcher task thread wait()时异常：", e);
                }
            }
        }
    });

    private void dispatcherTask() {
        synchronized (lock) {
          lock.notify();
        }
    }

    public void stopTask(){
        isStopTask = true;
        synchronized (lock) {
            lock.notifyAll();
        }
        requests.shutdown();
    }

    public void refreshCube(final String cubeName, final Date startTime, final Date endTime, final Callback callback) {
        callCubeApi(cubeName, startTime, endTime, "REFRESH", callback);
    }

    public void rebuildCube(final String cubeName, final Date startTime, final Date endTime, final Callback callback) {
        callCubeApi(cubeName, startTime, endTime, "BUILD", callback);
    }

    private void callCubeApi(final String cubeName, final Date startTime, final Date endTime, final String buildType, final Callback callback) {
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
                });
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

    public static class CubeTask {
        public String cubeName;
        public Date startTime;
        public Date endTime;

        public CubeTask(String cubeName, Date startTime, Date endTime) {
            this.cubeName = cubeName;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        @Override
        public int hashCode() {
            return (cubeName + String.valueOf(startTime.getTime()) + String.valueOf(endTime.getTime())).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CubeTask) {
                CubeTask c = (CubeTask) obj;
                return cubeName.equals(c.cubeName) && startTime.equals(c.startTime) && endTime.equals(c.endTime);
            }
            return false;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        KylinClient kylinClient = new KylinClient(
                "http://node14:7070/kylin/api",
                "ADMIN",
                "KYLIN");
//        kylinClient.rebuildCube("cube_ssp_traffic_overall_dm_final", new Date(1451750400000L), new Date(1451836800000L));

        kylinClient.refreshOrBuildCubeAtLeastOnceTask("cube_ssp_traffic_overall_dm_final",
                "2012-09-09 22:22:22",
                "2012-09-10 22:22:22"
        );

        Thread.sleep(1000*5);
        kylinClient.stopTask();

//
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
