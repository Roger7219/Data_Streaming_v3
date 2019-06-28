spark-submit \
--name ndsp_overall \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/ndsp/overall/overall.conf \
--jars \
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
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.app.name=ndsp_overall \
--conf spark.yarn.executor.memoryOverhead=2g  \
/apps/data-streaming/ndsp/overall/data-streaming.jar \
overall.conf buration = 150 kill=true modules = ndsp_overall_traffic,ndsp_overall_performance offset=latest

#version=2 offset=latest

[program:ndsp_overall]
directory=/apps/data-streaming/ndsp/overall
command=spark-submit --name ndsp_overall --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 2 --num-executors 1 --queue default --files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/ndsp/overall/overall.conf --jars file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,file:///apps/data-streaming/libs/config-1.3.1.jar,file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar --verbose --conf spark.ui.port=1111 --conf spark.app.name=ndsp_overall --conf spark.yarn.executor.memoryOverhead=2g  /apps/data-streaming/ndsp/overall/data-streaming.jar overall.conf buration = 300 kill=true modules = ndsp_overall_traffic,ndsp_overall_performance
priority=1
user=root
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/apps/data-streaming/log/ndsp_overall.log
stdout_logfile_maxbytes=100MB
stdout_logfile_backups=10

delete from rollbackable_transaction_cookie where module_name = 'ndsp_overall_traffic';
delete from rollbackable_transaction_cookie where module_name = 'ndsp_overall_performance';
delete from  offset where module_name='ndsp_overall_traffic';
delete from  offset where module_name='ndsp_overall_performance';