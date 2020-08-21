spark-submit --name ssp_dwr_overall \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 10g \
--executor-cores 5 \
--num-executors 6 \
--queue default \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.2.1.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.2.1.jar,\
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
hdfs:/libs/kafka_2.11-0.11.0.1.jar,\
hdfs:/libs/kafka-clients-0.11.0.1.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.yarn.executor.memoryOverhead=2g \
--files /apps/data-streaming/ssp_dwr_overall/app.conf \
/apps/data-streaming/ssp_dwr_overall/data-streaming.jar \
app.conf \
buration = 300 \
rate=4000000 \
kill=true \
version=4