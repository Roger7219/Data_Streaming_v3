cd /apps/data-streaming/ndsp/ck;
spark-submit \
--name ndsp_ck \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/ndsp/ck/ck.conf \
--jars \
file:///apps/data-streaming/libs/okio-1.14.0.jar,\
file:///apps/data-streaming/libs/httpcore-4.4.jar,\
file:///apps/data-streaming/libs/httpmime-4.4.jar,\
file:///apps/data-streaming/libs/httpclient-4.0.1.jar,\
file:///apps/data-streaming/libs/guava-20.0.jar,\
file:///apps/data-streaming/libs/clickhouse-jdbc-0.1.39.jar,\
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ndsp_ck \
--conf spark.yarn.executor.memoryOverhead=2g \
/apps/data-streaming/ndsp/ck/data-streaming.jar ck.conf kill=true buration=60 \
modules = ck_ndsp_overall_dm



[program:ndsp_ck]
directory=/apps/data-streaming/ndsp/ck
command=spark-submit --name ndsp_ck --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 2 --num-executors 1 --queue default --files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/ndsp/ck/ck.conf --jars file:///apps/data-streaming/libs/okio-1.14.0.jar,file:///apps/data-streaming/libs/httpcore-4.4.jar,file:///apps/data-streaming/libs/httpmime-4.4.jar,file:///apps/data-streaming/libs/httpclient-4.0.1.jar,file:///apps/data-streaming/libs/guava-20.0.jar,file:///apps/data-streaming/libs/clickhouse-jdbc-0.1.39.jar,file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,file:///apps/data-streaming/libs/config-1.3.1.jar,file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,file:///apps/data-streaming/libs/hbase-client-1.1.10.jar --verbose --conf spark.ui.port=1112 --conf spark.conf.app.name=ndsp_ck --conf spark.yarn.executor.memoryOverhead=2g /apps/data-streaming/ndsp/ck/data-streaming.jar ck.conf kill=true buration=60 modules = ck_ndsp_overall_dm
priority=1
user=root
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/apps/data-streaming/log/ndsp_ck.log
stdout_logfile_maxbytes=100MB
stdout_logfile_backups=10
