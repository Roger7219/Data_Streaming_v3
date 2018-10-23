### yarn-client
spark-submit \
--name ssp_mix \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 2 \
--queue queueK \
--jars file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=3777 \
--conf spark.conf.app.name=ssp_mix \
/root/kairenlo/data-streaming/ssp/mix/data-streaming.jar \
/root/kairenlo/data-streaming/ssp/mix/mix.conf


### yarn-cluster
cd /root/kairenlo/data-streaming/ssp/mix;
spark-submit \
--name ssp_mix \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 10g \
--executor-cores 4 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/dw_dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=3777 \
--conf spark.conf.app.name=ssp_mix \
--conf spark.task.maxFailures=100 \
--conf spark.driver.extraClassPath=protobuf-java-3.0.0.jar \
--conf spark.executor.extraClassPath=protobuf-java-3.0.0.jar \
--files /root/kairenlo/data-streaming/ssp/mix/mix.conf,/root/kairenlo/data-streaming/ssp/mix/smartdatavo.conf,/root/kairenlo/data-streaming/ssp/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/mix/user.conf,/root/kairenlo/data-streaming/ssp/mix/show.conf,/root/kairenlo/data-streaming/ssp/mix/send.conf,/root/kairenlo/data-streaming/ssp/mix/log.conf,/root/kairenlo/data-streaming/ssp/mix/image.conf,/root/kairenlo/data-streaming/ssp/mix/fill.conf,/root/kairenlo/data-streaming/ssp/mix/fee.conf,/root/kairenlo/data-streaming/ssp/mix/dsp.conf,/root/kairenlo/data-streaming/ssp/mix/click.conf,/root/kairenlo/data-streaming/ssp/mix/postback.conf \
/root/kairenlo/data-streaming/ssp/mix/data-streaming.jar \
mix.conf kill=true buration = 20



rebrush=running

spark-submit --name ssp_mix --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 6g --executor-memory 6g --executor-cores 4 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/dw_dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.port=3777 --conf spark.conf.app.name=ssp_mix --conf spark.task.maxFailures=100 --conf spark.driver.extraClassPath=protobuf-java-3.0.0.jar --conf spark.executor.extraClassPath=protobuf-java-3.0.0.jar --files /root/kairenlo/data-streaming/ssp/mix/mix.conf,/root/kairenlo/data-streaming/ssp/mix/smartdatavo.conf,/root/kairenlo/data-streaming/ssp/mix/dupscribe.conf,/root/kairenlo/data-streaming/ssp/mix/user.conf,/root/kairenlo/data-streaming/ssp/mix/show.conf,/root/kairenlo/data-streaming/ssp/mix/send.conf,/root/kairenlo/data-streaming/ssp/mix/log.conf,/root/kairenlo/data-streaming/ssp/mix/image.conf,/root/kairenlo/data-streaming/ssp/mix/fill.conf,/root/kairenlo/data-streaming/ssp/mix/fee.conf,/root/kairenlo/data-streaming/ssp/mix/dsp.conf,/root/kairenlo/data-streaming/ssp/mix/click.conf /root/kairenlo/data-streaming/ssp/mix/data-streaming.jar mix.conf kill=true buration = 200



#
#"bq_dupscribe.conf"
#  ,"dsp.conf"
#  ,"fee.conf"
#  ,"fill.conf"
#  ,"image.conf"
#
#  ,"log.conf"
#  ,"send2.conf"
#//  ,"/root/kairenlo/data-streaming/ssp_send_hbase/send_hbase.conf"
#  ,"show.conf"
#  ,"user.conf"