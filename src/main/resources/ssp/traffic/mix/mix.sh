### yarn-client
spark-submit \
--name ssp_traffic_mix \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue queueSqoop \
--jars file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=2777 \
--conf spark.conf.app.name=ssp_traffic_mix \
/root/kairenlo/data-streaming/ssp/traffic/mix/data-streaming.jar \
/root/kairenlo/data-streaming/ssp/traffic/mix/mix.conf


### yarn-cluster
cd /root/kairenlo/data-streaming/ssp/traffic/mix/;
spark-submit \
--name ssp_traffic_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=2777 \
--conf spark.conf.app.name=ssp_traffic_mix \
--conf spark.task.maxFailures=100 \
--files /root/kairenlo/data-streaming/ssp/traffic/mix/mix.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/publisher.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/offer.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/campaign.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_campaign.conf \
/root/kairenlo/data-streaming/ssp/traffic/mix/data-streaming.jar \
mix.conf buration = 900

 rebrush=running


spark-submit \
--name dw_campaign \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=2777 \
--conf spark.conf.app.name=dw_campaign \
--conf spark.task.maxFailures=100 \
--files /root/kairenlo/data-streaming/ssp/traffic/mix/mix.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/publisher.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/offer.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/campaign.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_app.conf,/root/kairenlo/data-streaming/ssp/traffic/mix/report_campaign.conf \
/root/kairenlo/data-streaming/ssp/traffic/mix/data-streaming.jar \
mix.conf  modules = cam_totalcost_fee,cam_totalcost_cpc,cam_totalcost_cpm,cam_day_fee,cam_day_clickcount_cpccost,cam_day_showcount_cpmcost buration=10

