package com.mobikok.ssp.data.streaming.util;

/**
 * Created by Administrator on 2017/8/17.
 */
public class StringUtil {
    public static boolean isEmpty(String value) {
        if(value == null || "".equals(value.trim())) return true;
        return false;
    }

    public static boolean notEmpty(String value) {
        return !isEmpty(value);
    }
}
