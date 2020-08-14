package com.mobikok.ssp.data.streaming.module.support;

public enum TimeGranularity {
    Day("day"),
    Month("month"),
    Default("default"); // default一般为小时，取决于module的b_time.format配置

    public final String name;

    TimeGranularity(String name){
        this.name = name;
    }

}
