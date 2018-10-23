package com.mobikok.ssp.data.streaming.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/12/20.
 */
public class UserAgentDeviceTypeUDF extends UDF implements Serializable {

    public String evaluate(Text str) {
        try {
            return UserAgent.parseUserAgentString(str.toString()).getOperatingSystem().getDeviceType().getName();
        } catch (Throwable e) {
            return "Unknown";//ExceptionUtils.getFullStackTrace(e);
        }
    }

}
