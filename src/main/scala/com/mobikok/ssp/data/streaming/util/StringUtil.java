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

    public static String getStackTraceMessage(Exception e) {
        StackTraceElement[] elements = e.getStackTrace();
        StringBuilder exceptionMessage = new StringBuilder();
        exceptionMessage.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
        for (StackTraceElement element : elements) {
            exceptionMessage.append("    at ")
                    .append(element.getClassName())
                    .append(".")
                    .append(element.getMethodName())
                    .append("(")
                    .append(element.getFileName());
            if (element.getLineNumber() != -1) {
                exceptionMessage.append(":").append(element.getLineNumber());
            }
            exceptionMessage.append(")\n");
        }
        return exceptionMessage.toString();
    }
}
