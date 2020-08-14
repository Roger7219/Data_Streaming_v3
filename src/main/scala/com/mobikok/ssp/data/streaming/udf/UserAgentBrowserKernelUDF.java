package com.mobikok.ssp.data.streaming.udf;

import com.mobikok.ssp.data.streaming.util.StringUtil;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/12/20.
 */
public class UserAgentBrowserKernelUDF extends UDF implements Serializable {

    public String evaluate(Text str) {
        try {
            String ua = str.toString();
            Browser browser = UserAgent.parseUserAgentString(ua).getBrowser();
            String v = browser.getVersion(ua).getMajorVersion();
            if(StringUtil.notEmpty(v)){
                return new StringBuffer().append(browser.getName()).append(' ').append(v).toString();
            }else {
                return browser.getName();
            }
        } catch (Throwable e) {
            return "Unknown";//ExceptionUtils.getFullStackTrace(e);
        }
    }

//    private static class StringUtil {
//        public static boolean isEmpty(String value) {
//            if(value == null || "".equals(value.trim())) return true;
//            return false;
//        }
//
//        public static boolean notEmpty(String value) {
//            return !isEmpty(value);
//        }
//    }
//
//    public static void main(String[] args) {
////        String s= "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.3325.181 Safari/537.36";
//        String s= "Mozilla/5.0 (Linux; Android 5.1; itel it1508 Build/LMY47D) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/50.0.2661.86 Mobile Safari/537.36";
////        String s = "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36";
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser().getName() + " " + UserAgent.parseUserAgentString(s).getBrowser().getVersion(s).getMajorVersion());
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser().getBrowserType());
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser().getName());
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser().getGroup());
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser().getGroup());
//        System.out.println(UserAgent.parseUserAgentString(s).getBrowser());
//    }
}
