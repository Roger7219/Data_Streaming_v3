#!/bin/bash
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
  lftp << EOF
open ftp://$USER:$PASS@$HOST
set ssl:verify-certificate no
get $deviceIdFile -o $deviceIdFile
EOF
  echo "$deviceIdFile download finished"
sleep 10s
    if [ -f $deviceIdFile ]; then
       echo "scp csv data to exchange"
       #awk -F, '{if($1!="")print $1","$5}'
       awk -F, 'NR>1{if($1!="" && $5>=0.3)print $1","$5}' $deviceIdFile > ivt_device_id.csv
       scp ivt_device_id.csv root@ex001:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex002:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex003:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex004:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex005:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex006:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex007:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex008:/data/nadx-exchange/
       scp ivt_device_id.csv root@ex001-sg:/data/nadx-exchange/
       #scp ivt_device_id.csv root@node245:/data/nadx-exchange/
       ##### redis start
         echo "black.ivt.deviceId redis start"
         redisFileName="redis_"`echo "ivt_device_id.csv" | awk -F. '{print $1}'`".txt"
         echo "del black.ivt.deviceId " > $redisFileName
         awk -F, '{print "sadd black.ivt.deviceId " tolower($1)}' ivt_device_id.csv >> $redisFileName
         if [ -f $redisFileName ]; then
           cat $redisFileName | redis-cli -h nadx-redis1g.redis.rds.aliyuncs.com --pipe
         fi
         echo "black.ivt.deviceId redis end"
       ##### redis end
       curTime=`date "+%Y-%m-%d %H:%M:%S"`
       message='[{"topic":"blackList_device_id_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
       curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d "$message"
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
        rename $deviceIdFile "$deviceIdFile".`date "+%Y.%m.%d.%H.%M.%S"` $deviceIdFile
    else
        echo "$deviceIdFile is exist"
    fi
fi
#############################IP 黑名单####################################
if [ ! -f $IPFile ]; then
  echo "$IPFile is empty . download GenericIPBlacklisting File..."
  lftp << EOF
open ftp://$USER:$PASS@$HOST
set ssl:verify-certificate no
get $IPFile -o $IPFile
EOF
  echo "$IPFile download finished"
sleep 10s
  if [ -f $IPFile ]; then
      echo "scp csv data to exchange"
      #awk -F, '{if($1!="")print $1","$5}'
      awk -F, 'NR>1{if($1!="")print $1","$3}' $IPFile > ivt_ip.csv
      scp ivt_ip.csv root@ex007:/data/nadx-exchange/
      scp ivt_ip.csv root@ex001:/data/nadx-exchange/
      scp ivt_ip.csv root@ex002:/data/nadx-exchange/
      scp ivt_ip.csv root@ex003:/data/nadx-exchange/
      scp ivt_ip.csv root@ex004:/data/nadx-exchange/
      scp ivt_ip.csv root@ex005:/data/nadx-exchange/
      scp ivt_ip.csv root@ex006:/data/nadx-exchange/
      scp ivt_ip.csv root@ex007:/data/nadx-exchange/
      scp ivt_ip.csv root@ex008:/data/nadx-exchange/
      scp ivt_ip.csv root@ex001-sg:/data/nadx-exchange/
      #scp ivt_ip.csv root@node245:/data/nadx-exchange/
      ##### redis start
        echo "black.ivt.ip redis start"
        redisFileName="redis_"`echo "ivt_ip.csv" | awk -F. '{print $1}'`".txt"
        echo "del black.ivt.ip " > $redisFileName
        awk -F, '{print "sadd black.ivt.ip " tolower($1)}' ivt_ip.csv >> $redisFileName
        if [ -f $redisFileName ]; then
          cat $redisFileName | redis-cli -h nadx-redis1g.redis.rds.aliyuncs.com --pipe 
        fi
        echo "black.ivt.ip redis end"
      ##### reids end
      curTime=`date "+%Y-%m-%d %H:%M:%S"`
      message='[{"topic":"blackList_ip_check_topic","key":"'$curTime'","uniqueKey":true,"data":""}]'
      curl $MESSAGE_URL --header  "Content-Type: application/json;charset=UTF-8" -d "$message"
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
        rename $IPFile "$deviceIdFile".`date "+%Y.%m.%d.%H.%M.%S"` $IPFile
    else
        echo "$IPFile is exist"
    fi
fi
for ((j = 20; j > 5; j--))
do
  rmFile="deviceidblacklistv2/DeviceIdBlacklist_`date -d ''$j' days ago' "+%Y%m%d".*`"
  rm -rf $rmFile
  rmFile="ipblocklistv2/GenericIPBlacklisting_`date -d ''$j' days ago' "+%Y%m%d".*`"
  rm -rf $rmFile
done
echo "script end at " `date "+%Y-%m-%d %H:%M:%S"`

