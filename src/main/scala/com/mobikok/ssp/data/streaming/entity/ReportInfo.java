package com.mobikok.ssp.data.streaming.entity;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/5/12 0012.
 */
public class ReportInfo implements Serializable {
    private int publisherId;
    private int appId;
    private int adId;
    private int ssprequests;
    private int sspsends;
    private int impressions;
    private int clicks;
    private String countryCode;
    private String mediaprice;
    private String b_date;

    public ReportInfo(int publisherId, int appId, int adId, int ssprequests, int sspsends, int impressions, int clicks, String countryCode, String mediaprice, String statDate) {
        this.publisherId = publisherId;
        this.appId = appId;
        this.adId = adId;
        this.ssprequests = ssprequests;
        this.sspsends = sspsends;
        this.impressions = impressions;
        this.clicks = clicks;
        this.countryCode = countryCode;
        this.mediaprice = mediaprice;
        this.b_date = b_date;
    }

    public int getPublisherId() {
        return publisherId;
    }
    public void setPublisherId(int publisherId) {
        this.publisherId = publisherId;
    }
    public int getAppId() {
        return appId;
    }
    public void setAppId(int appId) {
        this.appId = appId;
    }
    public int getAdId() {
        return adId;
    }
    public void setAdId(int adId) {
        this.adId = adId;
    }
    public int getSsprequests() {
        return ssprequests;
    }
    public void setSsprequests(int ssprequests) {
        this.ssprequests = ssprequests;
    }
    public int getSspsends() {
        return sspsends;
    }
    public void setSspsends(int sspsends) {
        this.sspsends = sspsends;
    }
    public int getImpressions() {
        return impressions;
    }
    public void setImpressions(int impressions) {
        this.impressions = impressions;
    }
    public int getClicks() {
        return clicks;
    }
    public void setClicks(int clicks) {
        this.clicks = clicks;
    }
    public String getCountryCode() {
        return countryCode;
    }
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }
    public String getMediaprice() {
        return mediaprice;
    }
    public void setMediaprice(String mediaprice) {
        this.mediaprice = mediaprice;
    }
    public String getB_date() {
        return b_date;
    }

    public void setB_date(String b_date) {
        this.b_date = b_date;
    }

    @Override
    public String toString() {
        return "ReportInfo{" +
                "publisherId=" + publisherId +
                ", appId=" + appId +
                ", adId=" + adId +
                ", ssprequests=" + ssprequests +
                ", sspsends=" + sspsends +
                ", impressions=" + impressions +
                ", clicks=" + clicks +
                ", countryCode='" + countryCode + '\'' +
                ", mediaprice='" + mediaprice + '\'' +
                ", b_date='" + b_date + '\'' +
                '}';
    }
}
