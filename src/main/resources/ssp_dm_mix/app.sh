spark-submit --name ssp_dm_mix \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-cluster \
--driver-memory 8g \
--executor-memory 3g \
--executor-cores 1 \
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
file:///apps/data-streaming/libs/quartz-2.3.0.jar,\
hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,\
hdfs:/libs/config-1.3.1.jar \
--driver-class-path /apps/data-streaming/libs/mysql-connector-java-5.1.42.jar \
--verbose \
--conf spark.driver.extraClassPath=guava-20.0.jar \
--conf spark.executor.extraClassPath=guava-20.0.jar \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=6 \
--conf spark.dynamicAllocation.initialExecutors=1 \
--conf spark.dynamicAllocation.executorIdleTimeout=20s \
--conf spark.dynamicAllocation.cachedExecutorIdleTimeout=20s \
--files /apps/data-streaming/ssp_dm_mix/key.json,/apps/data-streaming/ssp_dm_mix/app.conf \
/apps/data-streaming/ssp_dm_mix/data-streaming.jar \
app.conf \
modules=bq_app,bq_agg_traffic,bq_dsp,bq_dupscribe,bq_dupscribe_detail,bq_log,bq_offer,bq_top_offer,bq_user_keep,bq_user_na,bq_adx,redis_day_month_limit,monitor_userinfo,redis_offer_roi_ecpm,agg_traffic_month_dwr,ssp_report_campaign_month_dm,monitor_ecpec_cr,bq_topn,bq_bd_offer,bd_write_redis,bd_update_mysql_offer,bd_update_mysql_offer,offer_auto_soldout,bq_image,mysql_app,redis_app_image_info,mysql_adver,mysql_campaign,mysql_update_image,mysql_publisher,publisher_third_income,clicks2Redis,redis_offfer_country_stat,mysql_mode_caps \
kill=true