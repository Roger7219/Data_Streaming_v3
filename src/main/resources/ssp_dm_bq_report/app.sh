spark-submit --name ssp_dm_bq_report \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 4 \
--num-executors 2 \
--queue default \
--jars \
file:///apps/data-streaming/libs/google-api-client-1.22.0.jar,\
file:///apps/data-streaming/libs/google-api-services-bigquery-v2-rev347-1.22.0.jar,\
file:///apps/data-streaming/libs/google-auth-library-credentials-0.8.0.jar,\
file:///apps/data-streaming/libs/google-auth-library-oauth2-http-0.8.0.jar,\
file:///apps/data-streaming/libs/google-cloud-bigquery-0.25.0-beta.jar,\
file:///apps/data-streaming/libs/google-cloud-core-1.7.0.jar,\
file:///apps/data-streaming/libs/google-cloud-core-http-1.7.0.jar,\
file:///apps/data-streaming/libs/google-http-client-1.22.0.jar,\
file:///apps/data-streaming/libs/google-http-client-appengine-1.22.0.jar,\
file:///apps/data-streaming/libs/google-http-client-jackson-1.22.0.jar,\
file:///apps/data-streaming/libs/google-http-client-jackson2-1.22.0.jar,\
file:///apps/data-streaming/libs/google-java-format-1.0.jar,\
file:///apps/data-streaming/libs/google-oauth-client-1.22.0.jar,\
file:///apps/data-streaming/libs/GoogleBigQueryJDBC42.jar,\
file:///apps/data-streaming/libs/curator-client-2.8.0.jar,\
file:///apps/data-streaming/libs/commons-pool2-2.3.jar,\
file:///apps/data-streaming/libs/jedis-2.9.0.jar,\
file:///apps/data-streaming/libs/jodis-0.4.1.jar,\
file:///apps/data-streaming/libs/google-api-client-1.22.0.jar,\
file:///apps/data-streaming/libs/google-api-services-bigquery-v2-rev347-1.22.0.jar,\
file:///apps/data-streaming/libs/google-http-client-appengine-1.22.0.jar,\
file:///apps/data-streaming/libs/api-common-1.1.0.jar,\
file:///apps/data-streaming/libs/google-http-client-jackson2-1.22.0.jar,\
file:///apps/data-streaming/libs/google-http-client-jackson-1.22.0.jar,\
file:///apps/data-streaming/libs/guava-20.0.jar,\
file:///apps/data-streaming/libs/threetenbp-1.3.3.jar,\
file:///apps/data-streaming/libs/gax-1.8.1.jar,\
file:///apps/data-streaming/libs/google-auth-library-oauth2-http-0.8.0.jar,\
file:///apps/data-streaming/libs/google-auth-library-credentials-0.8.0.jar,\
file:///apps/data-streaming/libs/json-20160810.jar,\
file:///apps/data-streaming/libs/google-http-client-1.22.0.jar,\
file:///apps/data-streaming/libs/google-cloud-bigquery-0.25.0-beta.jar,\
file:///apps/data-streaming/libs/google-cloud-core-1.7.0.jar,\
file:///apps/data-streaming/libs/google-cloud-core-http-1.7.0.jar,\
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
--driver-class-path /apps/data-streaming/libs/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /apps/data-streaming/ssp_dm_bq_report/key.json,/apps/data-streaming/ssp_dm_bq_report/app.conf \
/apps/data-streaming/ssp_dm_bq_report/data-streaming.jar \
app.conf \
buration=120 \
kill=true