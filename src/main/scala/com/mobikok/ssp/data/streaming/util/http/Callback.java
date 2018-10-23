package com.mobikok.ssp.data.streaming.util.http;

import java.net.HttpURLConnection;

/**
 * Created by Administrator on 2017/7/12.
 */
public interface Callback {

    void prepare(HttpURLConnection conn);
    void completed(String responseStatus, String response);
    void failed(String responseStatus, String responseError, Exception ex);
}
