package com.mobikok.ssp.data.streaming.udf;

import com.mobikok.ssp.data.streaming.util.OM;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Administrator on 2017/12/20.
 */
public class UserAgentOperatingSystemUDF extends UDF implements Serializable {

    public String evaluate(Text str) {
        try {
//            return "HelloWorld " + str;
            return UserAgent.parseUserAgentString(str.toString()).getOperatingSystem().getName();
        } catch (Throwable e) {
            return "Unknown";//ExceptionUtils.getFullStackTrace(e);
        }
    }

//    public String evaluate(Text userAgentString) {
//        try {
//            return UserAgent.parseUserAgentString(userAgentString.toString()).getOperatingSystem().toString();
//        } catch (Exception e) {
//            return null;
//        }
//    }
}
