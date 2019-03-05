cd /apps/data-streaming/nadx/ck;
spark-submit \
--name ck \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--files \
/usr/hdp/current/spark2-client/conf/hive-site.xml,\
/apps/data-streaming/nadx/ck/ck.conf \
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
--conf spark.conf.app.name=ck \
/apps/data-streaming/nadx/ck/data-streaming.jar ck.conf kill=true buration=10 \
modules = ck_nadx_overall_dm




