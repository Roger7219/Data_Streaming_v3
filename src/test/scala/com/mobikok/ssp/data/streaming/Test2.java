package com.mobikok.ssp.data.streaming;

import com.mobikok.ssp.data.streaming.util.OM;

import java.util.Arrays;

public class Test2 {

    public static void main(String[] s) {

//        System.out.println(Math.log(81)/Math.log(3));
//
//        System.out.println(Math.log(100)/Math.log(3));

        double[] ds = new double[]{};

        System.out.println(OM.toJOSN(zoom(ds)));
//
//        System.out.println(Math.pow(2,1.5));
    }

    public static double[] zoom(double[] ds){
        double range = 10.0;

        if(ds.length == 0) { return ds;}
        if(ds.length == 1) { ds[0] = range; return ds; }

        Arrays.sort(ds);
        double _max = ds[ds.length - 1];
        double _min = ds[0];

        // 指数值
        double n = Math.log(range)/Math.log(_max/_min);

        double k = range/Math.pow(_max, n);

        for(int i = 0; i < ds.length; i++) {
            double d = Math.pow(ds[i], n);
            ds[i] = d * k;
        }

        return ds;
    }
}
