package com.mobikok.ssp.data.streaming.util;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Created by Administrator on 2017/7/13.
 */
public class Command {

    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(Command.class);

    public static String exeCmd(String commandStr) {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            String enc = System.getProperty("sun.jnu.encoding");

            Process p = Runtime.getRuntime().exec(commandStr);
            br = new BufferedReader(new InputStreamReader(p.getInputStream(), enc));
            String line = null;

            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
            //LOG.error("运行命令异常: ", e);

        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
//                    LOG.error("运行命令异常: ", ex);
                }
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) {
        String commandStr = "ping www.taobao.com";
        //String commandStr = "ipconfig";
        String  res = Command.exeCmd(commandStr);
        System.out.println(res);
    }
}
