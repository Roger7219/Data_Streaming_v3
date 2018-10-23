package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;

/**
 * Created by Administrator on 2017/8/17.
 */
public class RespOne<T> extends JavaJSONSerializable {
    private Integer status = 0; //1成功，0异常
    private T data;
    private String error;

    public String getError() {
        return error;
    }
    public void setError(String error) {
        this.error = error;
    }

    public Integer getStatus() {
        return status;
    }
    public RespOne<T> setStatus(Integer status) {
        this.status = status;
        return this;
    }
    public T getData() {
        return data;
    }
    public void setData(T data) {
        this.data = data;
    }

}
