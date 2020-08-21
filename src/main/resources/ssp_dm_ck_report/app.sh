spark-submit --name ssp_dm_ck_report \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 7g \
--executor-cores 8 \
--num-executors 1 \
--queue default \
--jars \
file:///apps/data-streaming/libs/commons-pool2-2.3.jar,\
file:///apps/data-streaming/libs/jedis-2.9.0.jar,\
file:///apps/data-streaming/libs/jodis-0.4.1.jar,\
file:///apps/data-streaming/libs/okio-1.14.0.jar,\
file:///apps/data-streaming/libs/httpcore-4.4.4.jar,\
file:///apps/data-streaming/libs/httpclient-4.5.2.jar,\
file:///apps/data-streaming/libs/guava-20.0.jar,\
file:///apps/data-streaming/libs/clickhouse-jdbc-0.1.39.jar,\
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.2.1.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.2.1.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.10.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar \
--verbose \
--files /apps/data-streaming/ssp_dm_ck_report/app.conf \
/apps/data-streaming/ssp_dm_ck_report/data-streaming.jar \
app.conf \
kill=true \
buration=60
