package com.mobikok.ssp.data.streaming.util;

import com.mobikok.ssp.data.streaming.entity.YarnAppList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.*;


public class YarnAppManagerUtil {

    private static Logger LOG = Logger.getLogger(YarnAppManagerUtil.class);
    private static YarnClient client;

    static {
        yarnClientInit();
    }

    private static void yarnClientInit(){
        Configuration conf = new Configuration();
        client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
    }

    public static void main(String[] args) {

    }

    public static boolean isPrevRunningApp(String appName, String excludeLatestSetupAppId){

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

    public static void killApps(String appName, MessageClient messageClient) throws IOException, YarnException {
        killApps(appName, null, true, Collections.EMPTY_SET, messageClient);
    }
    public static void killApps(String appName, String specifiedAppId, MessageClient messageClient) throws IOException, YarnException {
        killApps(appName, specifiedAppId, true, Collections.EMPTY_SET, messageClient);
    }
    public static void killApps(String appName, boolean forceKill, String excludeAppId, MessageClient messageClient) throws IOException, YarnException {
        killApps(appName, null, forceKill, Collections.singleton(excludeAppId), messageClient);
    }

    /**
     * kill所有指定名字的yarn任务
     * @param appName
     * @throws IOException
     * @throws YarnException
     */
    public static void killApps(String appName, String specifiedAppId, boolean forceKill, Set<String> excludeAppIds, MessageClient messageClient) throws IOException, YarnException {
        LOG.warn("Kill apps START. appName: " + appName + ", specifiedAppId: " + specifiedAppId +", forceKill: " + forceKill + ", excludeAppIds: " + excludeAppIds);

        boolean notifiedKillMessage = false;

        while (true) {
            List<ApplicationReport> apps = getApps(appName);

            List<ApplicationReport> filteredApps = new ArrayList<ApplicationReport>();

            // 排除
            for(ApplicationReport app : apps) {
                if((StringUtil.isEmpty(specifiedAppId) || app.getApplicationId().toString().equals(specifiedAppId))
                    && !excludeAppIds.contains(app.getApplicationId().toString())) {

                    filteredApps.add(app);
                    LOG.warn("Waiting kill app complete, app: " + app.getApplicationId() +", forceKill: " + forceKill);
                }
            }

            if(filteredApps.size() == 0) {
                LOG.warn("Kill apps DONE. appName: " + appName + ", specifiedAppId: " + specifiedAppId +", forceKill: " + forceKill + ", excludeAppIds: " + excludeAppIds);
                return;
            }

            // Kill
            for (ApplicationReport app : filteredApps) {
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
            if(filteredApps.size() >= 1) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Kill apps error:", e);
                }
            }
        }

    }


    // 获取最近一次启动的app
    public static ApplicationReport getLatestSetupApp(String appName) throws IOException, YarnException{
        List<ApplicationReport> apps = getAppsOrderByStartTimeDesc(appName);
        return apps.size() > 0 ? apps.get(0) : null;
    }

    public static List<ApplicationReport> getAppsOrderByStartTimeDesc(String appName) throws IOException, YarnException{
        List<ApplicationReport> result = getApps(appName);

        result.sort(new Comparator<ApplicationReport>() {
            public int compare(ApplicationReport a, ApplicationReport b) {
                return a.getStartTime() > b.getStartTime() ? -1 : 1;
            }
        });

        return result;
    }

    // 按创建时间降序
    public static List<ApplicationReport> getApps(String appName) throws IOException, YarnException{
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