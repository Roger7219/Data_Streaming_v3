### yarn-client
spark-submit \
--name ssp_send2hbase \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 4g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=2555 \
--conf spark.conf.app.name=ssp_send2hbase \
/root/kairenlo/data-streaming/ssp/send2hbase/data-streaming.jar \
/root/kairenlo/data-streaming/ssp/send2hbase/send2hbase.conf



### yarn-cluster 正式的 SSP_SEND_DWI_PHOENIX_20180120
spark-submit \
--name send2hbase_new2 \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 10g \
--executor-cores 4 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/scala-library-2.11.8.jar,file:///root/kairenlo/data-streaming/data_lib/scala-reflect-2.11.8.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-api-jdo-3.2.6.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-core-3.2.10.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-rdbms-3.2.9.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=2635 \
--conf spark.app.name=send2hbase_new2 \
--files /root/kairenlo/data-streaming/ssp/send2hbase_new2/send2hbase.conf \
/root/kairenlo/data-streaming/ssp/send2hbase_new2/data-streaming.jar \
send2hbase.conf kill=true buration = 100


spark-submit --name send2hbase_new2 --class com.mobikok.ssp.data.streaming.MixApp --master yarn-cluster --driver-memory 10g --executor-memory 10g --executor-cores 4 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/scala-library-2.11.8.jar,file:///root/kairenlo/data-streaming/data_lib/scala-reflect-2.11.8.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-api-jdo-3.2.6.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-core-3.2.10.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-rdbms-3.2.9.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar --verbose --conf spark.ui.port=2635 --conf spark.app.name=send2hbase_new2 --files /root/kairenlo/data-streaming/ssp/send2hbase_new2/send2hbase.conf /root/kairenlo/data-streaming/ssp/send2hbase_new2/data-streaming.jar send2hbase.conf kill=true buration = 100



move '3b332030a89b03b085bbb937a3f03d76', 'node14,16020,1515372194757'




spark-submit \
--name send2hbase_new2 \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 10g \
--executor-cores 4 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/hive-exec-1.2.1.spark2.jar,file:///root/kairenlo/data-streaming/data_lib/scala-library-2.11.8.jar,file:///root/kairenlo/data-streaming/data_lib/scala-reflect-2.11.8.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-api-jdo-3.2.6.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-core-3.2.10.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-rdbms-3.2.9.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose \
--conf spark.ui.port=2635 \
--conf spark.app.name=send2hbase_new2 \
--files /root/kairenlo/data-streaming/ssp/send2hbase_new2/send2hbase.conf \
/root/kairenlo/data-streaming/ssp/send2hbase_new2/data-streaming.jar \
send2hbase.conf kill=true buration = 100
