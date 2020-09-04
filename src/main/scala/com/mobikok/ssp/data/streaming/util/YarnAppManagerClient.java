package com.mobikok.ssp.data.streaming.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;


public class YarnAppManagerClient {

    private Logger LOG;
    private MessageClient messageClient;
    private static YarnClient client;

    public YarnAppManagerClient(String loggerName, MessageClient messageClient){
        LOG = new Logger(loggerName, YarnAppManagerClient.class);
        this.messageClient = messageClient;
    }

    static {
        yarnClientInit();
    }

    private static void yarnClientInit(){
        Configuration conf = new Configuration();
        client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
    }

    public boolean isPrevRunningApp(String appName, String excludeLatestSetupAppId){

        try {
            List<ApplicationReport> prevRunningApps = new ArrayList<>();
            List<ApplicationReport> apps = getApps(appName);

            for(ApplicationReport a: apps){
                if(!a.getApplicationId().toString().equals(excludeLatestSetupAppId)) {
                    prevRunningApps.add(a);
                }
            }

            return !prevRunningApps.isEmpty();

        } catch (YarnException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return false ;
    }

    public void killApps(String appName) throws IOException, YarnException {
        killApps(appName, null, true, Collections.EMPTY_SET);
    }
    public void killApps(String appName, String specifiedAppId) throws IOException, YarnException {
        killApps(appName, specifiedAppId, true, Collections.EMPTY_SET);
    }
    public void killApps(String appName, boolean forceKill, String excludeAppId) throws IOException, YarnException {
        killApps(appName, null, forceKill, Collections.singleton(excludeAppId));
    }

    /**
     * kill所有指定名字的yarn任务
     * @param appName
     * @throws IOException
     * @throws YarnException
     */
    public void killApps(String appName, String specifiedAppId, boolean forceKill, Set<String> excludeAppIds) throws IOException, YarnException {
        LOG.warn("Kill apps START. appName: " + appName + ", specifiedAppId: " + specifiedAppId +", forceKill: " + forceKill + ", excludeAppIds: " + excludeAppIds);

        boolean notifiedKillMessage = false;

        while (true) {
            List<ApplicationReport> apps = getApps(appName);

            List<ApplicationReport> needKillApps = new ArrayList<ApplicationReport>();

            for(ApplicationReport app : apps) {
                // 排除指定app_id的app
                if(!excludeAppIds.contains(app.getApplicationId().toString())) {

                    // 如果指定了app_id，则kill掉指定app_id的app
                    if(StringUtil.notEmpty(specifiedAppId) && app.getApplicationId().toString().equals(specifiedAppId)) {
                        needKillApps.add(app);
                    }
                    // 否则kill掉指定app_name的所有同名apps
                    else {
                        needKillApps.add(app);
                    }

                }
            }

            if(needKillApps.size() == 0) {
                LOG.warn("Kill apps DONE. appName: " + appName + ", specifiedAppId: " + specifiedAppId +", forceKill: " + forceKill + ", excludeAppIds: " + excludeAppIds);
                return;
            }

            // Kill
            for (ApplicationReport app : needKillApps) {
                String currName = app.getName();
                String currId = app.getApplicationId().toString();

                //强制立即Kill
                if(forceKill) {
                    LOG.warn("Killing appName: " + currName + ", appId: " + currId + ", forceKill: " + forceKill);
                    client.killApplication(app.getApplicationId());
                // 通知Kill,并等待完成
                }else if(!notifiedKillMessage){
                    LOG.warn("Killing appName: " + currName + ", appId: " + currId + ", forceKill: " + forceKill);
                    messageClient.push(new PushReq("kill_self_" + currName, currId));
                    notifiedKillMessage = true;
                }
            }

            //稍等待yarn app关闭
            if(needKillApps.size() >= 1) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Kill apps error:", e);
                }
            }
        }

    }


    // 获取最近一次启动的app
    public ApplicationReport getLatestSetupApp(String appName) throws IOException, YarnException{
        List<ApplicationReport> apps = getAppsOrderByStartTimeDesc(appName);
        return apps.size() > 0 ? apps.get(0) : null;
    }

    public List<ApplicationReport> getAppsOrderByStartTimeDesc(String appName) throws IOException, YarnException{
        List<ApplicationReport> result = getApps(appName);

        result.sort(new Comparator<ApplicationReport>() {
            public int compare(ApplicationReport a, ApplicationReport b) {
                return a.getStartTime() > b.getStartTime() ? -1 : 1;
            }
        });

        return result;
    }

    // 按创建时间降序
    public List<ApplicationReport> getApps(String appName) throws IOException, YarnException{
        List<ApplicationReport> result = new ArrayList<ApplicationReport>();

        EnumSet<YarnApplicationState> appStates = EnumSet.of(
                YarnApplicationState.NEW,
                YarnApplicationState.NEW_SAVING,
                YarnApplicationState.SUBMITTED,
                YarnApplicationState.ACCEPTED,
                YarnApplicationState.RUNNING
        );

        List<ApplicationReport> appsReport = client.getApplications(appStates);

        for (ApplicationReport appReport : appsReport) {
            if(appReport.getName().equals(appName)) {
                result.add(appReport);
            }
        }

        return result;
    }
}