spark-submit --name ssp_dwi_hive \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 3 \
--queue default \
--jars \
file:///apps/data-streaming/libs/h2-1.4.197.jar,\
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
file:///apps/data-streaming/libs/quartz-2.3.0.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
hdfs:/libs/kafka_2.11-0.11.0.1.jar,\
hdfs:/libs/kafka-clients-0.11.0.1.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.dynamicAllocation.maxExecutors=8 \
--conf spark.dynamicAllocation.initialExecutors=1 \
--conf spark.dynamicAllocation.executorIdleTimeout=10s \
--conf spark.dynamicAllocation.cachedExecutorIdleTimeout=10s \
--files /apps/data-streaming/ssp_dwi_hive/app.conf \
/apps/data-streaming/ssp_dwi_hive/data-streaming.jar \
app.conf \
buration = 300 \
rate=3000000 \
kill=true