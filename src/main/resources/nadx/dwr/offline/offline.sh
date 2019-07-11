#######################################################
## cd /apps/data-streaming/nadx/dwr/offline; (./offline.sh "2019-07-10 00:00:00" "2019-07-10 23:00:00" &);tail -f offline.log
#######################################################

cd /apps/data-streaming/nadx/dwr/offline

##start_b_time="`date "+%Y-%m-%d 00:00:00" -d "-1 days"`"
##end_b_time="`date "+%Y-%m-%d 23:00:00" -d "-1 days"`"

start_b_time="$1"
end_b_time="$2"

# alter table nadx_overall_dwr add partition (l_time='2019-06-28 00:00:00',b_date='2019-06-28',b_time='2019-06-28 00:00:00',b_version='0');

# Clean
echo "" >./offline.log

beeline -u jdbc:hive2://master:10000/default \
--outputformat=vertical \
--hivevar table=nadx_overall_dwr \
--hivevar start_b_time="${start_b_time}" \
--hivevar end_b_time="${end_b_time}" \
-e 'alter table nadx_overall_dwr drop partition(b_time>="${start_b_time}", b_time<="${end_b_time}")' >> ./offline.log 2>&1


spark-sql --master yarn \
--executor-cores 4 \
--driver-memory  3g  \
--executor-memory 3G \
--num-executors 3 \
--hivevar start_b_time="${start_b_time}" \
--hivevar end_b_time="${end_b_time}" \
-f ./offline.sql  >> ./offline.log 2>&1

java -cp data-streaming.jar com.mobikok.ssp.data.streaming.CrontabClickhouseRefreshUtil "`date "+%Y-%m-%d" -d "-1 days"`" >>  ./offline.log