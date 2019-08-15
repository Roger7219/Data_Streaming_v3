spark-submit \
--name test_v5 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/test/test.conf \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.yarn.executor.memoryOverhead=1g  \
/apps/data-streaming/nadx/test/data-streaming.jar \
test.conf buration = 10 kill=true modules = test_a version=5 rollback=false



spark-submit \
--name nadx_scraper_dwi \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/test/dwi.conf \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.app.name=nadx_scraper_dwi \
--conf spark.yarn.executor.memoryOverhead=1g \
/apps/data-streaming/nadx/test/data-streaming.jar \
dwi.conf buration = 30 kill=true modules = nadx_scraper_dwi



drop table nadx_overall_dwr_v10;
drop table nadx_overall_dwr_v10__app_or_site_id__counter;
drop table nadx_overall_dwr_v10__bundle_or_domain__counter;
drop table nadx_overall_dwr_v10_m_dwr_traffic_v10_backup_completed_20190715_100000_177__4;
drop table nadx_overall_traffic_dwi_v10;
drop table nadx_overall_traffic_dwi_v10_m_dwr_traffic_v10_backup_progressing_20190726_141611_846__3;
drop table nadx_overall_traffic_dwi_v10_m_dwr_traffic_v10_backup_progressing_20190726_142903_792__3;
drop table nadx_overall_traffic_dwi_v10_m_dwr_traffic_v10_backup_progressing_20190726_143713_122__1;
drop table nadx_overall_performance_matched_dwi_v10;
drop table nadx_overall_performance_matched_dwi_v10_m_dwr_performance_v10_backup_completed_20190715_100000_177__1;
drop table nadx_overall_performance_matched_dwi_v10_m_dwr_performance_v10_backup_progressing_20190726_140821_806__1;
drop table nadx_overall_performance_matched_dwi_v10_m_dwr_performance_v10_backup_progressing_20190726_141611_846__1;
drop table nadx_overall_performance_matched_dwi_v10_m_dwr_performance_v10_backup_progressing_20190726_142903_792__1;

select count(1) from nadx_overall_dwr_v10;
select count(1) from nadx_overall_dwr_v10__app_or_site_id__counter;
select count(1) from nadx_overall_dwr_v10__bundle_or_domain__counter;
select count(1) from nadx_overall_traffic_dwi_v10;

truncate table nadx_overall_dwr_v10;
truncate table nadx_overall_dwr_v10__app_or_site_id__counter;
truncate table nadx_overall_dwr_v10__bundle_or_domain__counter;
truncate table nadx_overall_traffic_dwi_v10;


select count(app_or_site_id)

### BundleData
spark-submit \
--name adx_dwr_v18 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 4 \
--num-executors 3 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/test/dwr.conf \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.yarn.executor.memoryOverhead=2g  \
/apps/data-streaming/nadx/test/data-streaming.jar \
dwr.conf buration = 150 kill=true modules = dwr_traffic,dwr_performance version=18 version.topic=6 \
rate=3000000 \
rollback=false \
offset=latest \
ex=site_app_id,placement_id,city,carrier,os_version,device_brand,device_model,bundle,site_domain,adomain,crid \
#ex=site_app_id,placement_id,city,carrier,os_version,device_brand,device_model,bundle,site_domain,publisher_id,adomain,crid,bundle_or_domain \




select b_time, count(1) from nadx_overall_dwr_v11 group by b_time order by b_time desc;

truncate table nadx_overall_dwr_v11;
truncate table nadx_overall_dwr_v11__app_or_site_id__counter;
truncate table nadx_overall_dwr_v11__bundle_or_domain__counter;
truncate table nadx_overall_dwr_v11__publisher_id__counter;


正式测：
spark-submit \
--name adx_dwr_v18 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 4 \
--num-executors 3 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/test/dwr.conf \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.yarn.executor.memoryOverhead=2g  \
/apps/data-streaming/nadx/test/data-streaming.jar \
dwr.conf buration = 150 kill=true modules = dwr_traffic,dwr_performance version=18 version.topic=6 \
rate=3000000 \
rollback=false \
offset=latest \
ex=site_app_id,placement_id,city,carrier,os_version,device_brand,device_model,bundle,site_domain,adomain,crid




spark-submit \
--name adx_dwr_v18 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 4g \
--executor-cores 4 \
--num-executors 5 \
--queue default \
--files /usr/hdp/current/spark2-client/conf/hive-site.xml,/apps/data-streaming/nadx/test/dwr.conf \
--jars \
file:///apps/data-streaming/libs/metrics-core-2.2.0.jar,\
file:///apps/data-streaming/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
file:///apps/data-streaming/libs/kafka_2.11-0.10.1.0.jar,\
file:///apps/data-streaming/libs/kafka-clients-0.10.1.0.jar,\
file:///apps/data-streaming/libs/tephra-api-0.7.0.jar,\
file:///apps/data-streaming/libs/twill-discovery-api-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/twill-zookeeper-0.6.0-incubating.jar,\
file:///apps/data-streaming/libs/tephra-core-0.7.0.jar,\
file:///apps/data-streaming/libs/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///apps/data-streaming/libs/config-1.3.1.jar,\
file:///apps/data-streaming/libs/hbase-server-1.1.10.jar,\
file:///apps/data-streaming/libs/mysql-connector-java-5.1.42.jar,\
file:///apps/data-streaming/libs/hbase-protocol-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-common-1.1.3.jar,\
file:///apps/data-streaming/libs/hbase-client-1.1.10.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
hdfs:/libs/kafka_2.11-0.10.1.0.jar,\
hdfs:/libs/kafka-clients-0.10.1.0.jar,\
hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1111 \
--conf spark.yarn.executor.memoryOverhead=2g  \
/apps/data-streaming/nadx/test/data-streaming.jar \
dwr.conf buration = 150 kill=true modules = dwr_traffic,dwr_performance version=18 version.topic=6 \
rate=3000000 \
#offset=latest \
#rollback=false \
