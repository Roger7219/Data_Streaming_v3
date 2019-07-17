spark-submit --name adx_dwr_v6 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 8 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/nadx/dwr/dwr.conf \
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
--conf spark.yarn.executor.memoryOverhead=2g \
/apps/data-streaming/nadx/dwr/data-streaming.jar \
overall.conf buration = 150 kill=true modules = dwr_traffic,dwr_performance version=6 \
ex = site_app_id,placement_id,city,carrier,os_version,device_brand,device_model,bundle,site_domain,publisher_id,adomain,crid
#offset=latest




spark-submit --name adx_dwr_v10 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 8 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/nadx/test/dwr.conf \
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
--conf spark.yarn.executor.memoryOverhead=2g \
/apps/data-streaming/nadx/test/data-streaming.jar \
dwr.conf buration = 150 kill=true modules = dwr_traffic,dwr_performance version=10 \
ex = site_app_id,placement_id,city,carrier,os_version,device_brand,device_model,site_domain,publisher_id,adomain,crid offset=latest



