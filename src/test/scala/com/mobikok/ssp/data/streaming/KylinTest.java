package com.mobikok.ssp.data.streaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.binary.Base64;

/**
 * Created by Administrator on 2017/7/13.
 */
public class KylinTest {

    private static String account = "ADMIN";
    private static String password = "KYLIN";

    private String e = "does not match any existing segment in cube CUBE";

//    http://192.168.22.102:7070/kylin/api/cubes/KPI_Base_DataCppaCrcCount_test_Cube/rebuild

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.parse ("2012-12-12 12:13:12"));

                System.out.println(callRestApi(
                "http://node14:7070/kylin/api/cubes/cube_ssp_image_dm/rebuild",
                "{\"startTime\": "+df.parse ("2000-01-01 08:00:00").getTime()+
                        ",\"endTime\": "+df.parse ("9999-01-01 08:00:00").getTime()+",\"buildType\": \"REFRESH\"}"));

    }

    private static String callRestApi(String addr, String params) {
        System.out.println(params);
        String result = "";
        try {
            URL url = new URL(addr);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            String auth = account + ":" + password;
            String code = new String(new Base64().encode(auth.getBytes()));
            connection.setRequestProperty("Authorization", "Basic " + code);
            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.write(params);
            out.close();
            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(
                        connection.getInputStream()));
            } catch (Exception exception) {
                java.io.InputStream err = ((HttpURLConnection) connection)
                        .getErrorStream();
                if (err == null)
                    throw exception;
                in = new BufferedReader(new InputStreamReader(err));
            }
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null)
                response.append(line + "\n");
            in.close();

            result = response.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}
