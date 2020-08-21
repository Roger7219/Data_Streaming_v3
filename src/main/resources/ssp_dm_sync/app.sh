spark-submit \
--name ssp_dm_sync \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 7g \
--executor-cores 8 \
--num-executors 1 \
--queue default \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/greenplum.jar,\
file:///apps/data-streaming/libs/postgresql-42.1.1.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--driver-class-path /apps/data-streaming/libs/mysql-connector-java-5.1.42.jar \
--verbose   \
--conf spark.yarn.executor.memoryOverhead=3g \
--files /apps/data-streaming/ssp_dm_sync/app.conf \
/apps/data-streaming/ssp_dm_sync/data-streaming.jar \
app.conf \
kill = true \
buration = 120