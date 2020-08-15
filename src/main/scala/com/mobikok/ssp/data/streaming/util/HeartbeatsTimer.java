package com.mobikok.ssp.data.streaming.util;

public class HeartbeatsTimer {
    Logger LOG = new Logger(getClass().getSimpleName(), getClass());
    private long intervalTimeMs;
    private long lastReportTime = 0;

    public HeartbeatsTimer(){
        this(120);
    }
    public HeartbeatsTimer(long intervalTimeSecond){
        this.intervalTimeMs = intervalTimeSecond * 1000;
    }


    public boolean isTimeToLog(){
        boolean b = System.currentTimeMillis() - lastReportTime >= intervalTimeMs;
        if(b){
            lastReportTime = System.currentTimeMillis();
        }
        return b;
    }

}
