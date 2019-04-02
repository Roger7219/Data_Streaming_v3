spark-sql --master yarn --executor-cores 12 --driver-memory  3g   --executor-memory 3G --num-executors 3 --hivevar start_b_time="`date "+%Y-%m-%d %H:00:00" -d "-1 hours"`" --hivevar end_b_time="`date "+%Y-%m-%d %H:00:00" -d "0 hours"`" --hivevar b_date="`date "+%Y-%m-%d" -d "0 days"`" -f ~/offline.crontab.hour.sql  > ~/offline.hour.log 2>&1

cd ~/offline
java -cp data-streaming.jar com.mobikok.ssp.data.streaming.CrontabClickhouseRefreshUtil "`date "+%Y-%m-%d" -d "0 days"`" >>  ~/offline.hour.log