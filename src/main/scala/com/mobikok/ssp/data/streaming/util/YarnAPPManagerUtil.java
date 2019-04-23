package com.mobikok.ssp.data.streaming.util;

import com.mobikok.message.MessagePushReq;
import com.mobikok.message.client.MessageClient;
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
import java.util.Map.Entry;

  
public class YarnAPPManagerUtil {

    private static Logger LOG = Logger.getLogger(YarnAPPManagerUtil.class);
    private static YarnClient client;
    private static YarnAppList applist;
    private static List<YarnAppList> applists = new ArrayList<YarnAppList>();

    private static String needToBeKillsAppId = "";

    static {
        yarnClientInit();
    }

    protected static PrintStream sysout = System.out;
    private static final String APPLICATIONS_PATTERN = "%30s\t%20s\t%20s\t%10s\t%10s\t%18s\t%18s\t%15s\t%35s"
            + System.getProperty("line.separator");

    private static MessageClient messageClient = new MessageClient("", "http://104.250.136.138:5555");

    public static void main(String[] args) {

//    	Map<String, String> nameIdPair = new HashMap<String,String>();
//        try {
//            YarnAPPManagerUtil app = new YarnAPPManagerUtil();
//            app.testAppState();
//
//            nameIdPair = getNameIdMap(getAppList());
//
//        	for(Entry<String, String> a : nameIdPair.entrySet()){
//        		System.out.println(a.getKey()+" "+a.getValue());
//        	}
//
//        	needToBeKillsAppId = nameIdPair.get("dw_test");
//            killApplication(needToBeKillsAppId);
//
////            runCommand("/usr/bin/ls /");
//
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
    } 

    @Deprecated
    public static boolean isAppRunning(String appName){

        try {
            List<YarnAppList> appLists = getRunningAppList();

            for (YarnAppList appList:appLists) {

                if(appList.getApplicationName().equals(appName)){
                    return true ;
                }
            }
        } catch (YarnException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false ;
    }

    @Deprecated
    public String getAppIdByAppName(String appName, Map<String, String> idNamePair){
		return idNamePair.get(appName);
	}
    
    public static Map<String, String> getNameIdMap( List<YarnAppList> applists) {
		
    	Map<String, String> nameIdMap = new HashMap<String, String>() ;
    	for (YarnAppList yarnAppList : applists) {
    		nameIdMap.put(yarnAppList.getApplicationName(), yarnAppList.getApplicationId());
    	}
    	return nameIdMap;
	}

	@Deprecated
    public static List<YarnAppList> getAppList() throws YarnException, IOException {
    	
//    	yarnClientInit();
    	
    	EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);  
        if (appStates.isEmpty()) {  
            appStates.add(YarnApplicationState.RUNNING);  
            appStates.add(YarnApplicationState.ACCEPTED);  
            appStates.add(YarnApplicationState.SUBMITTED);  
        }  
        List<ApplicationReport> appsReport = client.getApplications(appStates);  
    	
        for (ApplicationReport appReport : appsReport) {

            applist = new YarnAppList(appReport.getApplicationId().toString(), appReport.getName(),
            		appReport.getApplicationType(),
            		appReport.getYarnApplicationState().toString(), appReport.getFinalApplicationStatus().toString());
            if (null != applist) {
            	applists.add(applist);
			}

		}
		return applists;
	}

	@Deprecated
    public static List<YarnAppList> getRunningAppList() throws YarnException, IOException {

//        yarnClientInit();

        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
//            appStates.add(YarnApplicationState.ACCEPTED);
//            appStates.add(YarnApplicationState.SUBMITTED);
        }
        List<ApplicationReport> appsReport = client.getApplications(appStates);

        for (ApplicationReport appReport : appsReport) {

            applist = new YarnAppList(appReport.getApplicationId().toString(), appReport.getName(),
                    appReport.getApplicationType(),
                    appReport.getYarnApplicationState().toString(), appReport.getFinalApplicationStatus().toString());
            if (null != applist) {
                applists.add(applist);
            }

        }
        return applists;
    }



    private static void yarnClientInit(){  
        Configuration conf = new Configuration();  
        client = YarnClient.createYarnClient();  
        client.init(conf);  
        client.start();
    }
  
    private void testAppState() throws YarnException, IOException, InterruptedException, ClassNotFoundException {  
        Configuration conf = new Configuration();  
        client = YarnClient.createYarnClient();  
        client.init(conf);  
        client.start();  
      
        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);  
        if (appStates.isEmpty()) {  
            appStates.add(YarnApplicationState.NEW);
            appStates.add(YarnApplicationState.NEW_SAVING);
            appStates.add(YarnApplicationState.SUBMITTED);
            appStates.add(YarnApplicationState.ACCEPTED);
            appStates.add(YarnApplicationState.RUNNING);
        }
        List<ApplicationReport> appsReport = client.getApplications(appStates);

        PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Charset.forName("UTF-8")));  
        for (ApplicationReport appReport : appsReport) {  
            ApplicationReportPBImpl app = (ApplicationReportPBImpl) appReport;  
            DecimalFormat formatter = new DecimalFormat("###.##%");  
            String progress = formatter.format(appReport.getProgress());  
            writer.printf(APPLICATIONS_PATTERN, appReport.getApplicationId(), appReport.getName(),  
                    appReport.getApplicationType(), appReport.getUser(), appReport.getQueue(),  
                    appReport.getYarnApplicationState(), appReport.getFinalApplicationStatus(), progress,  
                    appReport.getOriginalTrackingUrl());  
        }  
        writer.flush();  
        for (ApplicationReport appReport : appsReport) {  
            String type = appReport.getApplicationType();  
            if(type.equalsIgnoreCase("spark")){  
                continue;  
            }  
            getStatusByAppId(appReport);  
        }  
  
    }  
    private void getStatusByAppId(ApplicationReport app){  
        String user = app.getUser();  
        ApplicationId id = app.getApplicationId();  
        String appId = app.getApplicationId().toString();  
        System.out.println(appId);  
          
    }

    @Deprecated
    private static void killApplication(String applicationId) throws YarnException, IOException{  
        if(null == applicationId || (0 == applicationId.length()) )
        	return ;
    	ApplicationId appId = ConverterUtils.toApplicationId(applicationId);  
        ApplicationReport  appReport = null;  
        try {  
          appReport = client.getApplicationReport(appId);  
        } catch (ApplicationNotFoundException e) {  
          sysout.println("Application with id '" + applicationId +  
              "' doesn't exist in RM.");  
          throw e;  
        }  
  
        if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED  
            || appReport.getYarnApplicationState() == YarnApplicationState.KILLED  
            || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {  
          sysout.println("Application " + applicationId + " has already finished ");  
        } else {  
          sysout.println("Killing application " + applicationId);  
          client.killApplication(appId);  
        }  
    }  
  
    public static int  runCommand (String commands) throws InterruptedException, IOException {
    	
    	Process process = Runtime.getRuntime().exec(commands);
    	return process.waitFor();
	}
    
    private void getAppState() throws Exception {  
        String[] args = { "-list" };  
        ApplicationCLI.main(args);  
    }

//    public static void killAppsExcludeSelf(String appName) throws IOException, YarnException {
//        killAppsExcludeSelf(appName, null, true);
//    }
//
//    public static void killAppsExcludeSelf(String appName, String appId) throws IOException, YarnException {
//        killAppsExcludeSelf(appName, appId, true);
//    }
//
//    // 排除最新启动的
//    public static void killAppsExcludeSelf(String appName, String appId, boolean forceKill) throws IOException, YarnException {
//        String latestApp = getLatestRunningApp(appName).getApplicationId().toString();
//        LOG.warn("Kill apps (exclude self) start. appName: " + appName + ", selfAppId" + latestApp +", force" + forceKill);
//        killApps(appName, appId, forceKill, latestApp);
//        LOG.warn("Kill apps (exclude self) done. appName: " + appName + ", selfAppId" + latestApp +", force" + forceKill);
//    }

    public static void killApps(String appName) throws IOException, YarnException {
        killApps(appName, null, true, Collections.EMPTY_SET);
    }
    public static void killApps(String appName, String specifiedAppId) throws IOException, YarnException {
        killApps(appName, specifiedAppId, true, Collections.EMPTY_SET);
    }
    public static void killApps(String appName, boolean forceKill, String excludeAppId) throws IOException, YarnException {
        killApps(appName, null, forceKill, Collections.singleton(excludeAppId));
    }

    /**
     * kill所有指定名字的yarn任务
     * @param appName
     * @throws IOException
     * @throws YarnException
     */
    public static void killApps(String appName, String specifiedAppId, boolean forceKill, Set<String> excludeAppIds) throws IOException, YarnException {
        LOG.warn("Kill apps START. appName: " + appName + ", specifiedAppId: " + specifiedAppId +", forceKill: " + forceKill + ", excludeAppIds: " + excludeAppIds);

//        yarnClientInit();
        boolean notifiedKillMessage = false;

        while (true) {
            List<ApplicationReport> apps = getRunningApps(appName);

            List<ApplicationReport> filteredApps = new ArrayList<ApplicationReport>();

            // 排除
            for(ApplicationReport app : apps) {
                if((StringUtil.isEmpty(specifiedAppId) || app.getApplicationId().toString().equals(specifiedAppId))
                    && !excludeAppIds.contains(app.getApplicationId().toString())) {

                    filteredApps.add(app);
                    LOG.warn("Killing filteredApp: " + app.getApplicationId() +", forceKill: " + forceKill);
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
                    messageClient.pushMessage(new MessagePushReq("kill_self_" + currName, currId));
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


    public static ApplicationReport getLatestRunningApp(String appName) throws IOException, YarnException{
        List<ApplicationReport> apps = getRunningAppsOrderByStartTimeDesc(appName);
        return apps.size() > 0 ? apps.get(0) : null;
    }

    public static List<ApplicationReport> getRunningAppsOrderByStartTimeDesc(String appName) throws IOException, YarnException{
        return getRunningApps(appName);
    }

    // 按创建时间降序
    public static List<ApplicationReport> getRunningApps(String appName) throws IOException, YarnException{
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

        result.sort(new Comparator<ApplicationReport>() {
            public int compare(ApplicationReport a, ApplicationReport b) {
                return a.getStartTime() > b.getStartTime() ? -1 : 1;
            }
        });

        return result;
    }
}