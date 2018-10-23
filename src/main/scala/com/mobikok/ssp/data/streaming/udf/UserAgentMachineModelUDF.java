package com.mobikok.ssp.data.streaming.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/12/20.
 */
public class UserAgentMachineModelUDF extends UDF implements Serializable {
///Mozilla/5.0 (Linux; Android 5.1; Micromax Q413 Build/LMY47D; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36
    private static Pattern pattern = Pattern.compile(";\\s?[^;\\s]*?(\\s?\\S*?)\\s?(Build)?/");


    public String evaluate(Text str) {
//        try {
//            String model[] =  getMachineModel(str.toString()).split(";");
//            return prominent(model[model.length - 1]);
//        } catch (Throwable e) {
//            return "exception";//ExceptionUtils.getFullStackTrace(e);
//        }
        return "Unknown";
    }
//
//    public String evaluate(Text str) {
//        try {
//            String model[] =  getMachineModel(str.toString()).split(";");
//            return model[model.length - 1];
//        } catch (Throwable e) {
//            return "exception";//ExceptionUtils.getFullStackTrace(e);
//        }
//    }

    public static String getMachineModel(String agent){
        if(agent.toLowerCase().indexOf("iPhone".toLowerCase()) != -1){
            return "iPhone";
        }
        Matcher matcher = pattern.matcher(agent);
        String model = null;
        if (matcher.find()) {
            model = matcher.group(1).trim();
            return model;
        }
        return null;
    }

    public static void main(String[] args) {
        String model[] = "U;Android4.1.2;tr-tr;GT-I9300".split(";");
        System.out.println(model[model.length - 1]);

//        System.out.println(getMachineModel("Mozilla/5.0 (Linux; Android 5.1; Micromax Q413 Build/LMY47D; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36"));
    }
}
