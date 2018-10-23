package com.mobikok.message;

import com.mobikok.ssp.data.streaming.entity.feature.JSONSerializable;
import com.mobikok.ssp.data.streaming.entity.feature.JavaJSONSerializable;
import org.codehaus.janino.Java;

/**
 * Created by Administrator on 2017/8/17.
 */
public class Resp<T> extends JavaJSONSerializable {
    private Integer status = 0; //1成功，0异常
    private T pageData;
    private Long total; //数据总条数
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
    public Resp<T> setStatus(Integer status) {
        this.status = status;
        return this;
    }
    public T getPageData() {
        return pageData;
    }
    public Resp<T> setPageData(T pageData) {
        this.pageData = pageData;
        return this;
    }
    public Long getTotal() {
        return total;
    }
    public Resp<T> setTotal(Long total) {
        this.total = total;
        return this;
    }
}
