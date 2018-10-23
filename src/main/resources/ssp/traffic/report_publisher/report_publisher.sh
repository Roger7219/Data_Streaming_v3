### yarn-client
spark-submit \
ssp_report_publisher
--name report_publisher \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5777 \
--conf spark.conf.app.name=report_publisher \
/root/kairenlo/data-streaming/ssp/traffic/report_publisher/report_publisher.conf
/root/kairenlo/data-streaming/ssp/traffic/report_publisher/data-streaming.jar \


### yarn-cluster
spark-submit \
--name ssp_report_publisher \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5777 \
--files /root/kairenlo/data-streaming/ssp/traffic/report_publisher/report_publisher.conf \
/root/kairenlo/data-streaming/ssp/traffic/report_publisher/data-streaming.jar \
report_publisher.conf buration=300


spark-submit --name ssp_report_publisher --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 6g --executor-memory 6g --executor-cores 1 --num-executors 1 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.port=5777 --files /root/kairenlo/data-streaming/ssp/traffic/report_publisher/report_publisher.conf /root/kairenlo/data-streaming/ssp/traffic/report_publisher/data-streaming.jar report_publisher.conf buration=300
