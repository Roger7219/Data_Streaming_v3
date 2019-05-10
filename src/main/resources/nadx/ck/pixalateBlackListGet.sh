#!bin/bash
echo "script start at " `date "+%Y-%m-%d %H:%M:%S"`
deviceIdFile="deviceidblacklistv2/DeviceIdBlacklist_"`date -d '1 days ago' "+%Y%m%d".csv`
IPFile="ipblocklistv2/GenericIPBlacklisting_"`date -d '1 days ago' "+%Y%m%d".csv`
MESSAGE_URL='http://104.250.136.138:5555/Api/Message'
HOST='ftp.pixalate.com'
USER='mobikok'
PASS='mBkoMobIP01_pixalateFtp'
CC_PASS='9036011d8fa79028f99488c2de6fdfb09d66beb5e4b8ca5178ee9ff9179ed6a8'
LCD='pixalate/blacklist'
if [ ! -d pixalate ]; then
  mkdir pixalate
fi
if [ ! -d $LCD ]; then
  mkdir $LCD
fi
cd $LCD
if [ ! -d ipblocklistv2 ]; then
  mkdir ipblocklistv2
fi
if [ ! -d deviceidblacklistv2 ]; then
  mkdir deviceidblacklistv2
fi
pwd

#############################DeviceId 黑名单####################################
if [ ! -f $deviceIdFile ]; then
  echo "$deviceIdFile is empty . download DeviceIdBlacklist File..."
   clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_device_id_raw"
   clickhouse-client  -m  --password $CC_PASS --query="CREATE TABLE blacklist_device_id_raw ( device_id String, fraudType Nullable(String), os Nullable(String), idType Nullable(String), probability Float64 DEFAULT CAST(0. AS Float64) ) ENGINE = MergeTree ORDER BY (device_id)  SETTINGS index_granularity = 8192;"
  loop start_device_id_download
  lftp << EOF
open ftp://$USER:$PASS@$HOST
set ssl:verify-certificate no
get $deviceIdFile -o $deviceIdFile
EOF
  echo "$deviceIdFile download finished"
sleep 10s
    if [ -f $deviceIdFile ]; then
       echo "import csv data to clickhouse blacklist_device_id_raw"
       clickhouse-client  -m  --password $CC_PASS --query="INSERT INTO blacklist_device_id_raw FORMAT CSVWithNames" < $deviceIdFile
       clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_device_id_raw_select_all"
       clickhouse-client  -m  --password $CC_PASS --query="create table blacklist_device_id_raw_select_all as blacklist_device_id_raw"
       clickhouse-client  -m  --password $CC_PASS --query="insert into blacklist_device_id_raw_select_all select * from blacklist_device_id_raw"
       curTime=`date "+%Y-%m-%d %H:%M:%S"`
       echo  '[{"topic":"blackList_device_id_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
       curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d '[{"topic":"blackList_device_id_topic","key":"`$curTime`","uniqueKey":true,"data":""}]'
   fi
else
    RMFILE=`lftp $USER:$PASS@$HOST -e "set ssl:verify-certificate no;ls $deviceIdFile;exit"`
    echo $RMFILE
    RMFILE_TIME="`echo $RMFILE|cut -d ' ' -f 8`"
    echo $RMFILE_TIME
    LFILE_TIME="`ls -l --time-style='+%H:%M' $deviceIdFile | cut -d ' ' -f 6`"
    LFILE_TIME=`date '+%H:%M' -d "$LFILE_TIME -7 hours"`
    echo $LFILE_TIME
    if [ "$LFILE_TIME" != "$RMFILE_TIME" ]; then
        echo "$deviceIdFile is updated"
        rm -rf $deviceIdFile
    else
        echo "$deviceIdFile is exist"
    fi
fi
#############################IP 黑名单####################################
if [ ! -f $IPFile ]; then
  echo "$IPFile is empty . download GenericIPBlacklisting File..."
   clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_ip_raw"
   clickhouse-client  -m  --password $CC_PASS --query="CREATE TABLE blacklist_ip_raw ( ip String, fraudType Nullable(String), probability Float64 DEFAULT CAST(0. AS Float64) ) ENGINE = MergeTree ORDER BY (ip)  SETTINGS index_granularity = 8192;"
  loop start_ip_download
  lftp << EOF
open ftp://$USER:$PASS@$HOST
set ssl:verify-certificate no
get $IPFile -o $IPFile
EOF
  echo "$IPFile download finished"
sleep 10s
  if [ -f $IPFile ]; then
      echo "import csv data to clickhouse blacklist_ip_raw"
      clickhouse-client  -m  --password $CC_PASS --query="INSERT INTO blacklist_ip_raw FORMAT CSVWithNames" < $IPFile
      clickhouse-client  -m  --password $CC_PASS --query="drop table if EXISTS blacklist_ip_raw_select_all"
      clickhouse-client  -m  --password $CC_PASS --query="create table blacklist_ip_raw_select_all as blacklist_ip_raw"
      clickhouse-client  -m  --password $CC_PASS --query="insert into  blacklist_ip_raw_select_all select * from blacklist_ip_raw"
      curTime=`date "+%Y-%m-%d %H:%M:%S"`
      echo  '[{"topic":"blackList_ip_check_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
      curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d '[{"topic":"blackList_ip_check_topic","key":"`$curTime`","uniqueKey":true,"data":""}]'
  fi
else
    RMFILE=`lftp $USER:$PASS@$HOST -e "set ssl:verify-certificate no;ls $IPFile;exit"`
    echo $RMFILE
    RMFILE_TIME="`echo $RMFILE|cut -d ' ' -f 8`"
    echo $RMFILE_TIME
    LFILE_TIME="`ls -l --time-style='+%H:%M' $IPFile | cut -d ' ' -f 6`"
    LFILE_TIME=`date '+%H:%M' -d "$LFILE_TIME -7 hours"`
    echo $LFILE_TIME
    if [ "$LFILE_TIME" != "$RMFILE_TIME" ]; then
        echo "$IPFile is updated"
        rm -rf $IPFile
    else
        echo "$IPFile is exist"
    fi
fi
echo "script end at " `date "+%Y-%m-%d %H:%M:%S"`
