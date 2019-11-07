#######################################################
## cd /apps/data-streaming/ndsp/overall/offline; (sh ./offline.sh "2019-09-14" &);tail -f offline.log
## cd /apps/data-streaming/ndsp/overall/offline; (sh ./offline.sh "2019-10-14" &);tail -f offline.log
## cd /apps/data-streaming/ndsp/overall/offline; (sh ./offline.sh "2019-10-30" &);tail -f offline.log
## cd /apps/data-streaming/ndsp/overall/offline; (sh ./offline.sh "2019-10-31" &);tail -f offline.log
## cd /apps/data-streaming/ndsp/overall/offline; (sh ./offline.sh "2019-11-03" &);tail -f offline.log
#######################################################

cd /apps/data-streaming/ndsp/overall/offline

##start_b_time="`date "+%Y-%m-%d 00:00:00" -d "-1 days"`"
##end_b_time="`date "+%Y-%m-%d 23:00:00" -d "-1 days"`"
start_b_date="$1"
if [ -z "$2" ]; then
  end_b_date=$1
else
  end_b_date="$2"
fi
start_b_date="`date "+%Y-%m-%d" -d "-1 days"`"
end_b_date="`date "+%Y-%m-%d" `"

echo "" >./offline.log

start_date=`date -d "$start_b_date" +%Y%m%d`
end_date=`date -d "$end_b_date" +%Y%m%d`

while [ "$start_date" -le "$end_date" ];
do
	stat_date=`date -d "$start_date" +%Y-%m-%d`
        stat_time="$stat_date 00:00:00"
	stat_date_num=`date -d "$start_date" +%Y%m%d`
	echo $stat_date
        spark-sql \
        --executor-cores 4 \
        --driver-memory  3g  \
        --executor-memory 3G \
        --num-executors 3 \
        --hivevar start_b_date="${stat_date}" \
        --hivevar start_b_time="${stat_time}" \
        -f ./offline.sql  >> ./offline.log 2>&1
	start_date=$(date -d "$start_date+1days" +%Y%m%d)
        sqoop export   --hcatalog-database default   --hcatalog-table ndsp_overall_day_dwr   --hcatalog-partition-keys b_date   --hcatalog-partition-values stat_date   --connect jdbc:mysql://218.17.186.161:3305/dsp    --username root   --password sloth --m 10  --table ndsp_overall_day_dwr --update-key id --update-mode allowinsert
done
