
cd /root/kairenlo/data-streaming/ck;
spark-submit \
--name ck_report_overall \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 7g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/ck/libs/okio-1.14.0.jar,file:///root/kairenlo/data-streaming/ck/libs/httpmime-4.5.2.jar,file:///root/kairenlo/data-streaming/ck/libs/httpcore-4.4.jar,file:///root/kairenlo/data-streaming/ck/libs/httpclient-4.5.jar,file:///root/kairenlo/data-streaming/ck/libs/guava-20.0.jar,file:///root/kairenlo/data-streaming/ck/libs/clickhouse-jdbc-0.1.39.jar,file:///root/kairenlo/data-streaming/ck/libs/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/ck/libs/config-1.3.1.jar,file:///root/kairenlo/data-streaming/ck/libs/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ck_report_overall \
--files /root/kairenlo/data-streaming/ck/clickhouse.conf \
/root/kairenlo/data-streaming/ck/data-streaming.jar clickhouse.conf modules = ck_report_overall kill=true buration=300



cd /root/kairenlo/data-streaming/ck;
spark-submit \
--name ck_overall_import_month \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 7g \
--executor-memory 7g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/ck/libs/okio-1.14.0.jar,file:///root/kairenlo/data-streaming/ck/libs/httpmime-4.5.2.jar,file:///root/kairenlo/data-streaming/ck/libs/httpcore-4.4.jar,file:///root/kairenlo/data-streaming/ck/libs/httpclient-4.5.jar,file:///root/kairenlo/data-streaming/ck/libs/guava-20.0.jar,file:///root/kairenlo/data-streaming/ck/libs/clickhouse-jdbc-0.1.39.jar,file:///root/kairenlo/data-streaming/ck/libs/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/ck/libs/config-1.3.1.jar,file:///root/kairenlo/data-streaming/ck/libs/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ck_overall_import_month \
--files /root/kairenlo/data-streaming/ck/clickhouse.conf \
/root/kairenlo/data-streaming/ck/data-streaming-month.jar clickhouse.conf modules = ck_overall_import_month kill=true buration=300


cd /root/kairenlo/data-streaming/ck;
spark-submit \
--name ck_dsp_cloak \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/ck/libs/okio-1.14.0.jar,file:///root/kairenlo/data-streaming/ck/libs/httpmime-4.5.2.jar,file:///root/kairenlo/data-streaming/ck/libs/httpcore-4.4.jar,file:///root/kairenlo/data-streaming/ck/libs/httpclient-4.5.jar,file:///root/kairenlo/data-streaming/ck/libs/guava-20.0.jar,file:///root/kairenlo/data-streaming/ck/libs/clickhouse-jdbc-0.1.39.jar,file:///root/kairenlo/data-streaming/ck/libs/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/ck/libs/config-1.3.1.jar,file:///root/kairenlo/data-streaming/ck/libs/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ck_dsp_cloak \
--files /root/kairenlo/data-streaming/ck/clickhouse.conf \
/root/kairenlo/data-streaming/ck/data-streaming-ck.jar clickhouse.conf modules = ck_dsp_cloak_conversion_dwi,ck_dsp_cloak_request_dwi kill=true buration=300
