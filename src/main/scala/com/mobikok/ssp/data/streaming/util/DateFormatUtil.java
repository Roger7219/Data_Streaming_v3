package com.mobikok.ssp.data.streaming.util;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by Administrator on 2017/12/27.
 */
public class DateFormatUtil {

    //北京时区
    public static SimpleDateFormat CST(String format){
        SimpleDateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return df;
    }
}
