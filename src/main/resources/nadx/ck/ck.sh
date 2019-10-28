cd /apps/data-streaming/nadx/ck;
spark-submit \
--name nadx_ck \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 10 \
--num-executors 1 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/nadx/ck/ck.conf \
--jars \
file:///apps/data-streaming/libs/alluxio-2.0.1-client.jar,\
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
--conf spark.conf.app.name=nadx_ck \
--conf spark.yarn.executor.memoryOverhead=2g \
/apps/data-streaming/nadx/ck/data-streaming.jar ck.conf kill=true buration=60 \
modules = ck_nadx_overall_dm,ck_nadx_overall_dm_v2,ck_nadx_overall_audit_dm,ck_nadx_overall_dm_v9


cd /apps/data-streaming/nadx/ck;
spark-submit \
--name nadx_ck_v6 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/nadx/ck/ck.conf \
--jars \
file:///apps/data-streaming/libs/alluxio-2.0.1-client.jar,\
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
--conf spark.yarn.executor.memoryOverhead=2g \
/apps/data-streaming/nadx/ck/data-streaming.jar ck.conf kill=true buration=60 \
modules = ck_nadx_overall_dm_v6


[program:nadx_ck]
directory=/apps/data-streaming/nadx/
command=spark-submit --name nadx_ck --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 10 --num-executors 1 --queue default --files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/ck/ck.conf --jars file:///apps/data-streaming/libs/alluxio-2.0.1-client.jar,file:///apps/data-streaming/libs/okio-1.14.0.jar,file:///apps/data-streaming/libs/httpcore-4.4.jar,file:///apps/data-streaming/libs/httpmime-4.4.jar,file:///apps/data-streaming/libs/httpclient-4.0.1.jar,file:///apps/data-streaming/libs/guava-20.0.jar,file:///apps/data-streaming/libs/clickhouse-jdbc-0.1.39.jar,file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,file:///apps/data-streaming/libs/config-1.3.1.jar,file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,file:///apps/data-streaming/libs/hbase-client-1.1.10.jar --verbose --conf spark.ui.port=1112 --conf spark.conf.app.name=nadx_ck --conf spark.yarn.executor.memoryOverhead=2g /apps/data-streaming/nadx/ck/data-streaming.jar ck.conf kill=true buration=60 modules = ck_nadx_overall_dm,ck_nadx_overall_audit_dm
priority=1
user=root
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/apps/data-streaming/log/nadx_ck.log
stdout_logfile_maxbytes=100MB
stdout_logfile_backups=10

