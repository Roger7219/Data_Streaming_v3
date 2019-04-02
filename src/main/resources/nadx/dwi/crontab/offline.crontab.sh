spark-sql --master yarn --executor-cores 12 --driver-memory  3g   --executor-memory 3G --num-executors 3 --hivevar start_b_time="`date "+%Y-%m-%d 00:00:00" -d "-1 days"`" --hivevar end_b_time="`date "+%Y-%m-%d 23:00:00" -d "-1 days"`" --hivevar b_date="`date "+%Y-%m-%d" -d "-1 days"`" -f ~/offline.crontab.sql  > ~/offline.log 2>&1

cd ~/offline
java -cp data-streaming.jar com.mobikok.ssp.data.streaming.CrontabClickhouseRefreshUtil "`date "+%Y-%m-%d" -d "-1 days"`" >>  ~/offline.log