### report_campaign
spark-submit \
--name ssp_report_campaign \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--files /root/kairenlo/data-streaming/ssp/traffic/report_campaign/report_campaign.conf \
/root/kairenlo/data-streaming/ssp/traffic/report_campaign/data-streaming.jar \
report_campaign.conf


### ssp/traffic/mix
cd /root/kairenlo/data-streaming/ssp/traffic/mix/;
spark-submit \
--name ssp_traffic_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=2777 \
--conf spark.task.maxFailures=100 \
--files /root/kairenlo/data-streaming/ssp/traffic/mix/mix.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/publisher.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/offer.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/campaign.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_campaign.conf \
/root/kairenlo/data-streaming/ssp/traffic/mix/data-streaming.jar \
mix.conf


### agg/mix
spark-submit \
--name agg_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 3 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=9399 \
--files /root/kairenlo/data-streaming/agg/mix/mix.conf,/root/kairenlo/data-streaming/agg/mix/user.conf,/root/kairenlo/data-streaming/agg/mix/fill.conf,/root/kairenlo/data-streaming/agg/mix/send.conf,/root/kairenlo/data-streaming/agg/mix/show.conf,/root/kairenlo/data-streaming/agg/mix/click.conf,/root/kairenlo/data-streaming/agg/mix/fee.conf,/root/kairenlo/data-streaming/agg/mix/traffic.conf \
/root/kairenlo/data-streaming/agg/mix/data-streaming.jar \
mix.conf


### agg/user
spark-submit \
--name agg_user \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=9199 \
--files /root/kairenlo/data-streaming/agg/user/user.conf \
/root/kairenlo/data-streaming/agg/user/data-streaming.jar \
user.conf



### ssp_mix
spark-submit \
--name ssp_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/dw_dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=3777 \
--conf spark.task.maxFailures=100 \
--conf spark.driver.extraClassPath=protobuf-java-3.0.0.jar \
--conf spark.executor.extraClassPath=protobuf-java-3.0.0.jar \
--files /root/kairenlo/data-streaming/ssp/mix/mix.conf,/root/kairenlo/data-streaming/ssp/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/mix/user.conf,/root/kairenlo/data-streaming/ssp/mix/show.conf,/root/kairenlo/data-streaming/ssp/mix/send.conf,/root/kairenlo/data-streaming/ssp/mix/log.conf,/root/kairenlo/data-streaming/ssp/mix/image.conf,/root/kairenlo/data-streaming/ssp/mix/fill.conf,/root/kairenlo/data-streaming/ssp/mix/fee.conf,/root/kairenlo/data-streaming/ssp/mix/dsp.conf,/root/kairenlo/data-streaming/ssp/mix/click.conf \
/root/kairenlo/data-streaming/ssp/mix/data-streaming.jar \
mix.conf



### ssp/image
spark-submit \
--name ssp_image \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=1777 \
--files /root/kairenlo/data-streaming/ssp/image/image.conf \
/root/kairenlo/data-streaming/ssp/image/data-streaming.jar \
image.conf



### adx
spark-submit \
--name adx \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/SspEvent.jar,file:///root/kairenlo/data-streaming/data_lib/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.driver.extraClassPath=protobuf-java-3.0.0.jar \
--conf spark.executor.extraClassPath=protobuf-java-3.0.0.jar \
--files /root/kairenlo/data-streaming/adx/adx.conf \
/root/kairenlo/data-streaming/adx/data-streaming.jar \
adx.conf


### ssp_send
spark-submit \
--name ssp_send \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=5555 \
--files /root/kairenlo/data-streaming/ssp/send/send.conf \
/root/kairenlo/data-streaming/ssp/send/data-streaming.jar \
send.conf

### ssp_traffic_publisher
spark-submit \
--name ssp_traffic_publisher \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5777 \
--files /root/kairenlo/data-streaming/ssp/traffic/publisher/publisher.conf \
/root/kairenlo/data-streaming/ssp/traffic/publisher/data-streaming.jar \
publisher.conf


### ssp_dupscribe
spark-submit \
--name ssp_dupscribe \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=9599 \
--files /root/kairenlo/data-streaming/ssp/dupscribe/dupscribe.conf \
/root/kairenlo/data-streaming/ssp/dupscribe/data-streaming.jar \
dupscribe.conf


### ssp_show
spark-submit \
--name ssp_show \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=9999 \
--files /root/kairenlo/data-streaming/ssp/show/show.conf \
/root/kairenlo/data-streaming/ssp/show/data-streaming.jar \
show.conf


### ssp_report_publisher
spark-submit \
--name ssp_report_publisher \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5777 \
--files /root/kairenlo/data-streaming/ssp/traffic/report_publisher/report_publisher.conf \
/root/kairenlo/data-streaming/ssp/traffic/report_publisher/data-streaming.jar \
report_publisher.conf

### ssp_click
spark-submit \
--name ssp_click \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=8888 \
--files /root/kairenlo/data-streaming/ssp/click/click.conf \
/root/kairenlo/data-streaming/ssp/click/data-streaming.jar \
click.conf

### ssp_fill
spark-submit \
--name ssp_fill \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=7777 \
--files /root/kairenlo/data-streaming/ssp/fill/fill.conf \
/root/kairenlo/data-streaming/ssp/fill/data-streaming.jar \
fill.conf


### yarn-cluster
spark-submit \
--name sync \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5777 \
--files /root/kairenlo/data-streaming/sync/sync.conf \
/root/kairenlo/data-streaming/sync/data-streaming.jar \
sync.conf



### ssp_send2hbase
spark-submit \
--name ssp_send2hbase \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=2555 \
--files /root/kairenlo/data-streaming/ssp/send2hbase/send2hbase.conf \
/root/kairenlo/data-streaming/ssp/send2hbase/data-streaming.jar \
send2hbase.conf
