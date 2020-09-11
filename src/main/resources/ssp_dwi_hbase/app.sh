spark-submit \
--name ssp_dwi_hbase \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 6 \
--queue default \
--jars \
file:///apps/data-streaming/libs/scala-library-2.11.8.jar,\
file:///apps/data-streaming/libs/scala-reflect-2.11.8.jar,\
file:///apps/data-streaming/libs/datanucleus-api-jdo-3.2.6.jar,\
file:///apps/data-streaming/libs/datanucleus-core-3.2.10.jar,\
file:///apps/data-streaming/libs/datanucleus-rdbms-3.2.9.jar,\
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
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar \
--verbose \
--files /apps/data-streaming/ssp_dwi_hbase/app.conf \
/apps/data-streaming/ssp_dwi_hbase/data-streaming.jar \
app.conf \
modules=send2hbase,send2hbase_key_subId_offerId,smartlink2hbase,events2hbase,postback2hbase \
kill=true


#2020年9月11日 Aaron 新添加modules:send2hbase_key_subId_offerId