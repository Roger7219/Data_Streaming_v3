package com.mobikok.ssp.data.streaming.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/8/17.
 */
public class StringUtil {

    public static void main(String[] args){
        System.out.println(getMatcher("\\$(.+?)\\.","sss$eeex.tt$t22.3"));
    }

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

    public static List<String> getMatcher(String regex, String source) {
        List<String> result = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(source);
        while (matcher.find()) {
            result.add(matcher.group(1));
        }
        return result;
    }

    public static String getFirstMatcher(String regex, String source) {
        try {
            return getMatcher(regex, source).get(0);
        }catch (Exception e) {
            return null;
        }
    }

    public static String standardizedSQL(String originalSQL) {
        return originalSQL
            .replaceAll("\\s+", " ")
            .replaceAll("\\.\\s*", ".")
            .replaceAll("\\s*=\\s*", "=")
            .replaceAll("\\s*<", "<")
            .replaceAll("\\s*>", ">")
            .replaceAll("\\$\\{\\s+", "${")
            .replaceAll("\\s+}+", "}")
            .replaceAll("\\s+,\\s+", "");

    }

}
