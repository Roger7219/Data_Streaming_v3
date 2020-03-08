spark-submit --name smartlink_dwi \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 8g \
--executor-memory 8g \
--executor-cores 3 \
--num-executors 2 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/smartlink/dwi/dwi.conf \
--jars \
file:///usr/hdp/current/alluxio/client/alluxio-1.8.2-client.jar,\
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar \
--conf spark.app.name=smartlink_dwi \
--conf spark.ui.port=1111 \
--conf spark.yarn.executor.memoryOverhead=2g \
--verbose \
/apps/data-streaming/smartlink/dwi/data-streaming.jar \
dwi.conf buration=300 kill=true \
rate=6000000

\
#offset=latest



[program:smartlink_dwi]
directory=/apps/data-streaming/smartlink/dwi
command=spark-submit --name smartlink_dwi --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 8g --executor-memory 8g --executor-cores 3 --num-executors 2 --queue default --files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/smartlink/dwi/dwi.conf --jars file:///usr/hdp/current/alluxio/client/alluxio-1.8.2-client.jar,file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,file:///apps/data-streaming/libs/config-1.3.1.jar,file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,file:///apps/data-streaming/libs/hbase-client-1.1.10.jar --conf spark.app.name=smartlink_dwi --conf spark.ui.port=1111 --conf spark.yarn.executor.memoryOverhead=2g --verbose /apps/data-streaming/smartlink/dwi/data-streaming.jar dwi.conf buration=300 kill=true rate=6000000
user=root
priority=1
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/apps/data-streaming/smartlink/dwi/log
stdout_logfile_maxbytes=1000MB
stdout_logfile_backups=10


