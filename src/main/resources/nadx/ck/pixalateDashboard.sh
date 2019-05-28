#!bin/bash
echo "script start at " `date "+%Y-%m-%d %H:%M:%S"` >>pixalateDashboard.log
MESSAGE_URL='http://104.250.136.138:5555/Api/Message'
CC_PASS='9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8'
LCD='pixalate/blacklist'
if [ ! -d pixalate ]; then
  mkdir pixalate
fi
if [ ! -d $LCD ]; then
  mkdir $LCD
fi
cd $LCD
if [ ! -d bundle ]; then
  mkdir bundle
fi
if [ ! -d domain ]; then
  mkdir domain
fi
pwd

#############################APP ID 黑名单####################################
timeRangeEnd=`date -d '0 days ago' "+%Y-%m-%d"`
timeRangeStart=`date -d '2 days ago' "+%Y-%m-%d"`
echo "http://dashboard.api.pixalate.com/services/2016/Report/getExportUri?username=d4df3f3506e476bdae08d3702910735d&password=7a9df2d4e11b31b80baeb69d641ab3f2&reportId=fraudAppId&timeZone=0&q=kv18%2CgivtSivtRate+WHERE+day>%3D'$timeRangeStart'+AND+day<%3D'$timeRangeEnd'+ORDER+BY+givtSivtRate+DESC&start=0&limit=999999&_1558700685545="
wget "http://dashboard.api.pixalate.com/services/2016/Report/getExportUri?username=d4df3f3506e476bdae08d3702910735d&password=7a9df2d4e11b31b80baeb69d641ab3f2&reportId=fraudAppId&timeZone=0&q=kv18%2CgivtSivtRate+WHERE+day>%3D'$timeRangeStart'+AND+day<%3D'$timeRangeEnd'+ORDER+BY+givtSivtRate+DESC&start=0&limit=999999&_1558700685545=" -O downloadBundlefilePath
downloadBundlefilePath=`cat downloadBundlefilePath |sed  s/\"//g`
if [ "$downloadBundlefilePath" ]; then
    appIDFileName="bundle/bundle_"`echo $downloadBundlefilePath|awk -F/ '{print $9}'`
    wget $downloadBundlefilePath -O $appIDFileName
    if [ -f $appIDFileName ]; then
       clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_bundle_raw"
       clickhouse-client  -m  --password $CC_PASS --query="CREATE TABLE blacklist_bundle_raw ( app_id String, probability Float64 DEFAULT CAST(0. AS Float64) ) ENGINE = MergeTree ORDER BY (app_id)  SETTINGS index_granularity = 8192;"
       clickhouse-client  -m  --password $CC_PASS --query="INSERT INTO blacklist_bundle_raw FORMAT CSVWithNames" < $appIDFileName
       clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_bundle_raw_select_all"
       clickhouse-client  -m  --password $CC_PASS --query="create table blacklist_bundle_raw_select_all as blacklist_bundle_raw"
       clickhouse-client  -m  --password $CC_PASS --query="insert into blacklist_bundle_raw_select_all select * from blacklist_bundle_raw"
       curTime=`date "+%Y-%m-%d %H:%M:%S"`
       message='[{"topic":"blackList_bundle_check_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
       curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d "$message"
    fi
fi
#############################domain 黑名单####################################
echo "http://dashboard.api.pixalate.com/services/2016/Report/getExportUri?username=d4df3f3506e476bdae08d3702910735d&password=7a9df2d4e11b31b80baeb69d641ab3f2&reportId=fraudDomain&timeZone=0&q=topAdDomain%2CgivtSivtRate+WHERE+day>%3D'$timeRangeStart'+AND+day<%3D'$timeRangeEnd'+ORDER+BY+topAdDomain+DESC&start=0&limit=999999&_1558700685545="
wget "http://dashboard.api.pixalate.com/services/2016/Report/getExportUri?username=d4df3f3506e476bdae08d3702910735d&password=7a9df2d4e11b31b80baeb69d641ab3f2&reportId=fraudDomain&timeZone=0&q=topAdDomain%2CgivtSivtRate+WHERE+day>%3D'$timeRangeStart'+AND+day<%3D'$timeRangeEnd'+ORDER+BY+topAdDomain+DESC&start=0&limit=999999&_1558700685545=" -O downloadDomainfilePath
downloadDomainfilePath=`cat downloadDomainfilePath |sed  s/\"//g`
if [ "$downloadDomainfilePath" ]; then
    domainFileName="domain/domain_"`echo $downloadDomainfilePath|awk -F/ '{print $9}'`
    wget $downloadDomainfilePath -O $domainFileName
    if [ -f $domainfileName ]; then
       clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_domain_raw"
       clickhouse-client  -m  --password $CC_PASS --query="CREATE TABLE blacklist_domain_raw ( domain String, probability Float64 DEFAULT CAST(0. AS Float64) ) ENGINE = MergeTree ORDER BY (domain)  SETTINGS index_granularity = 8192;"
       clickhouse-client  -m  --password $CC_PASS --query="INSERT INTO blacklist_domain_raw FORMAT CSVWithNames" < $domainFileName
       clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_domain_raw_select_all"
       clickhouse-client  -m  --password $CC_PASS --query="create table blacklist_domain_raw_select_all as blacklist_domain_raw"
       clickhouse-client  -m  --password $CC_PASS --query="insert into blacklist_domain_raw_select_all select * from blacklist_domain_raw"
       curTime=`date "+%Y-%m-%d %H:%M:%S"`
       message='[{"topic":"blackList_domain_check_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
       curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d "$message"
    fi
fi
for ((j = 20; j > 5; j--))
do
  rmFile="domain/domain_Report_from_*`date -d ''$j' days ago' "+%Y-%m-%d".*`"
  rm -rf $rmFile
  rmFile="bundle/bundle_Report_from_*`date -d ''$j' days ago' "+%Y-%m-%d".*`"
  rm -rf  $rmFile
done
echo "script end at " `date "+%Y-%m-%d %H:%M:%S"`
