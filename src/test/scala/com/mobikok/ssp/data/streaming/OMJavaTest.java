package com.mobikok.ssp.data.streaming;

import com.mobikok.message.client.MessageClientApi;
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart;
import com.mobikok.ssp.data.streaming.util.JavaMessageClient;
import com.mobikok.ssp.data.streaming.util.OM;

import java.util.List;

/**
 * Created by Administrator on 2017/8/10.
 */


public class OMJavaTest {
    public static void main(String[] args) {
        MessageClientApi c =new MessageClientApi("","http://node14:5555");

        JavaMessageClient.pullAndSortByLTimeDescHivePartitionParts(c, "c1", new JavaMessageClient.Callback<List<HivePartitionPart>>() {
            public Boolean doCallback(List<HivePartitionPart> resp){
                System.out.println(OM.toJOSN(resp));
                return true;
            }
        },"user_new");
//        String s2 = "{ " +
//                "\"repeats\" : null," +
//                "        \"rowkey\" : null," +
//                "\"id\" : null," +
//                "\"publisherId\" : null," +
//                "\"subId\" : null," +
//                "\"offerId\" : null," +
//                "\"campaignId\" : null," +
//                "           \"countryId\" : null," +
//                "        \"carrierId\" : null," +
//                "        \"deviceType\" : null," +
//                "        \"userAgent\" : null," +
//                "        \"ipAddr\" : null," +
//                "        \"clickId\" : null," +
//                "        \"price\" : 0.0," +
//                "        \"reportTime\" : null," +
//                "        \"createTime\" : null," +
//                "        \"clickTime\" : null," +
//                "        \"showTime\" : null," +
//                "        \"requestType\" : null," +
//                "        \"priceMethod\" : null," +
//                "        \"bidPrice\" : 0.0," +
//                "        \"adType\" : null," +
//                "        \"isSend\" : null," +
//                "        \"reportPrice\" : 0.0," +
//                "        \"sendPrice\" : 0.0," +
//                "        \"s1\" : null," +
//                "        \"s2\" : null," +
//                "        \"gaid\" : null," +
//                "        \"androidId\" : null," +
//                "        \"idfa\" : null," +
//                "        \"postBack\" : null," +
//                "        \"sendStatus\" : null," +
//                "        \"sendTime\" : null," +
//                "        \"sv\" : null," +
//                "        \"imei\" : null," +
//                "        \"imsi\" : null," +
//                "        \"imageId\" : 11," +
//                "        \"repeated\" : null," +
//                "        \"l_time\" : null," +
//                "        \"b_date\" : null," +
//                "        \"hbaseRowkey\" : 111" +
//                "}";
//
//        HBaseStorable ss = OM.toBean(s2, SspTrafficDWI.class);
//
//        System.out.println( (new HBaseStorable[]{ss}).getClass().getComponentType().getName());
//        System.out.println( (new HBaseStorable[]{ss}).getClass());

    }
}
