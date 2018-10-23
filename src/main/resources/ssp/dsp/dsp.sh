### yarn-client
spark-submit \
--name ssp_dsp \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 1 \
--queue queueC \
--jars file:///root/kairenlo/data-streaming/dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ssp_dsp \
/root/kairenlo/data-streaming/ssp/dsp/data-streaming.jar \
/root/kairenlo/data-streaming/ssp/dsp/dsp.conf



### yarn-cluster
spark-submit \
--name ssp_dsp \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=ssp_dsp \
--conf spark.driver.extraClassPath=protobuf-java-3.0.0.jar \
--conf spark.executor.extraClassPath=protobuf-java-3.0.0.jar \
--files /root/kairenlo/data-streaming/ssp/dsp/dsp.conf \
/root/kairenlo/data-streaming/ssp/dsp/data-streaming.jar \
dsp.conf

