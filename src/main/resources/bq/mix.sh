### yarn-client
spark-submit \
--name bq_mix \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 4g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/dw_dsp/SspEvent.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/dw_dsp/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/greenplum.jar,file:///root/kairenlo/data-streaming/data_lib/postgresql-42.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.2.1.jar,hdfs:/libs/kafka-clients-0.10.2.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=2555 \
--conf spark.conf.app.name=bq_mix \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
/root/kairenlo/data-streaming/bq.conf



spark-submit \
--name bq_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 3 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.app.name=bq_mix \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= \
bq_app, \
bq_agg_traffic, \
bq_dupscribe, \
bq_dupscribe_detail, \
bq_log, \
bq_offer, \
bq_report_campaign, \
bq_report_publisher, \
bq_top_offer, \
bq_user_keep, \
bq_user_na, \
bq_adx, \
mysql_publisher, mysql_campaign, mysql_adver, mysql_app, ssp_report_campaign_month_dm, \
bq_topn, bq_bd_offer, bd_write_redis, bd_update_mysql_offer, redis_day_month_limit, \
monitor_ecpec_cr, monitor_userinfo, redis_app_image_info, redis_offer_roi_ecpm, \
mysql_mode_caps, mysql_update_image, bq_image, agg_traffic_month_dwr,bq_report_overall,redis_programmed_bid  \
buration=300 kill=true


-------------------------------新拆分 start----------------------------------
spark-submit \
--name bq_mix \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 6 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.app.name=bq_mix \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= \
bq_app, \
bq_agg_traffic, \
bq_dupscribe, \
bq_dupscribe_detail, \
bq_log, \
bq_offer, \
bq_top_offer, \
bq_user_keep, \
bq_user_na, \
bq_adx, \
mysql_publisher, mysql_campaign, mysql_adver, mysql_app, \
bq_topn, bq_bd_offer, bd_write_redis, bd_update_mysql_offer, redis_day_month_limit, \
monitor_ecpec_cr, monitor_userinfo, redis_app_image_info, redis_offer_roi_ecpm, \
mysql_mode_caps, mysql_update_image, bq_image, agg_traffic_month_dwr,redis_programmed_bid,offer_auto_soldout  \
buration=300 kill=true


## bq_report_mix
spark-submit \
--name bq_report_mix \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.app.name=bq_report_mix \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= \
bq_report_campaign, \
bq_report_publisher, \
bq_report_overall \
buration=300 kill=true

spark-submit --name bq_report_mix --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 2 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.port=5771 --conf spark.app.name=bq_mix --conf spark.driver.extraClassPath=guava-20.0.jar --conf spark.executor.extraClassPath=guava-20.0.jar --files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf /root/kairenlo/data-streaming/bq/data-streaming.jar mix.conf modules= bq_report_campaign, bq_report_publisher, bq_report_overall buration=60 kill=true
# bq_report_overall独立出来
spark-submit --name bq_report_overall --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 1 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.port=5772 --conf spark.driver.extraClassPath=guava-20.0.jar --conf spark.executor.extraClassPath=guava-20.0.jar --files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf /root/kairenlo/data-streaming/bq/data-streaming.jar mix.conf modules= bq_report_overall buration=60 kill=true



# ssp_report_campaign_dwr_acc
spark-submit \
--name ssp_report_campaign_dwr_acc \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming2.jar \
mix.conf modules= ssp_report_campaign_dwr_acc \
buration=20 kill=true


# ssp_report_overall_dwr_day
spark-submit \
--name ssp_report_overall_dwr_day \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= ssp_report_overall_dwr_day \
buration=60 kill=true


#bq_report_overall_day_v2
spark-submit \
--name bq_report_overall_day_v2 \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=6771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= bq_report_overall_day_v2 \
buration=60 kill=true

spark-submit --name bq_report_overall_day_v2 --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 6g --executor-memory 6g --executor-cores 2 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.port=6771 --conf spark.driver.extraClassPath=guava-20.0.jar --conf spark.executor.extraClassPath=guava-20.0.jar --files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf /root/kairenlo/data-streaming/bq/data-streaming.jar mix.conf modules= bq_report_overall_day_v2 buration=60 kill=true


#bq_report_overall_day_v2_tmp
spark-submit \
--name bq_report_overall_day_v2_tmp \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=6771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= bq_report_overall_day_v2_tmp \
buration=60 kill=true


spark-submit \
--name publisher_third_income \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= publisher_third_income \
buration=20 kill=true

#

# bq_topn
# bq_bd_offer
# bd_write_redis
# bd_update_mysql_offer
# ssp_report_overall_dm_month
# ssp_report_overall_dwr_month
# offer_auto_soldout
# bq_image
# mysql_app
# redis_app_image_info
# mysql_adver
# mysql_campaign
# mysql_update_image

=== mysql_publisher ssp_report_overall_dwr_month
#bd_update_mysql_offer,offer_auto_soldout,bq_image,mysql_app,redis_app_image_info,mysql_adver,mysql_campaign,mysql_update_image

# ssp_report_overall_dwr_month
spark-submit \
--name ssp_report_overall_dwr_month \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules= ssp_report_overall_dwr_month \
buration=120 kill=true

# monitor_ecpec_cr
spark-submit \
--name monitor_ecpec_cr \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 5 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
/root/kairenlo/data-streaming/bq/data-streaming2.jar \
mix.conf modules= monitor_ecpec_cr \
buration=50 kill=true


#-- accelerate
#spark-submit \
#--name accelerate \
#--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
#--master yarn-cluster \
#--driver-memory 6g \
#--executor-memory 6g \
#--executor-cores 3 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.conf \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=ssp_report_campaign_month_dm \
#buration=120 kill=true


-------------------------------拆分 end----------------------------------

#
####topn bq_topn
#spark-submit \
#--name bq_topn \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 4g \
#--executor-memory 4g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=bq_topn \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=bq_topn

#
####topn bq_bd_offer
#spark-submit \
#--name bq_bd_offer \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=bq_bd_offer \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=bq_bd_offer buration=120
#
#
##### bd_write_redis
#spark-submit \
#--name bd_write_redis \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=bd_write_redis \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=bd_write_redis buration=300

#
##### bd_update_mysql_offer
#spark-submit \
#--name bd_update_mysql_offer \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=bd_update_mysql_offer \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=bd_update_mysql_offer buration=300


#
#spark-submit \
#--name redis_day_month_limit \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=redis_day_month_limit \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=redis_day_month_limit buration=300




#
#spark-submit \
#--name ssp_report_campaign_month_dm \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5773 \
#--conf spark.app.name=ssp_report_campaign_month_dm \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=ssp_report_campaign_month_dm buration=30

#bq_report_overall
spark-submit \
--name bq_report_overall \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.conf.app.name=bq_report_overall \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules=bq_report_overall  buration=100 kill=true


#bq_report_overall2
spark-submit \
--name bq_report_overall2 \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.conf.app.name=bq_report_overall \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules=bq_report_overall2  buration=100 kill=true


#redis_app_image_info
#spark-submit \
#--name redis_app_image_info \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=redis_app_image_info \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf modules=redis_app_image_info buration=10

#redis_offer_roi_ecpm

#spark-submit \
#--name redis_offer_roi_ecpm \
#--class com.mobikok.ssp.data.streaming.MixApp \
#--master yarn-cluster \
#--driver-memory 3g \
#--executor-memory 3g \
#--executor-cores 1 \
#--num-executors 1 \
#--queue default \
#--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
#--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
#--verbose \
#--conf spark.ui.port=5771 \
#--conf spark.conf.app.name=redis_offer_roi_ecpm \
#--conf spark.driver.extraClassPath=guava-20.0.jar \
#--conf spark.executor.extraClassPath=guava-20.0.jar \
#--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
#/root/kairenlo/data-streaming/bq/data-streaming.jar \
#mix.conf  kill=true modules=redis_offer_roi_ecpm, redis_app_image_info buration=20

spark-submit \
--name offer_auto_soldout \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.conf.app.name=offer_auto_soldout \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf  kill=true modules=offer_auto_soldout buration=300


spark-submit \
--name mysql_update_image \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.app.name=mysql_update_image \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf  kill=true modules=mysql_update_image buration=10


spark-submit \
--name bd_update_mysql_offer \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf  kill=true modules=bd_update_mysql_offer buration=10

--report_postback and report_events

spark-submit \
--name bq_report_postback_events \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.conf.app.name=bq_report_postback_events \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf modules=bq_report_events, bq_report_postback buration=10 kill=true


spark-submit \
--name clicks2Redis \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5771 \
--conf spark.app.name=clicks2Redis \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf  kill=true modules=clicks2Redis buration=300

mix.conf  kill=true modules=bq_image,update_mysql_image buration=100


#redis_offfer_country_stat
spark-submit \
--name redis_offfer_country_stat \
--class com.mobikok.ssp.data.streaming.MixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 1 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/curator-client-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.ui.port=5571 \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--files /root/kairenlo/data-streaming/bq/mix.conf,/root/kairenlo/data-streaming/bq/key.json \
/root/kairenlo/data-streaming/bq/data-streaming.jar \
mix.conf  kill=true modules=redis_offfer_country_stat buration=100
