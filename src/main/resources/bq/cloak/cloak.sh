[program:bq_report_mix]
directory=/root/kairenlo/data-streaming/bq
command=spark-submit --name bq_report_mix --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 3g --executor-memory 3g --executor-cores 2 --num-executors 2 --queue default --jars file:///root/kai
renlo/data-streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.
8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cl
oud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-cli
ent-appengine-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data
_lib/google-java-format-1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-oauth-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/GoogleBigQueryJDBC42.jar,file:///root/kairenlo/data-streaming/data_lib/curator-c
lient-2.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/commons-pool2-2.3.jar,file:///root/kairenlo/data-streaming/data_lib/jedis-2.9.0.jar,file:///root/kairenlo/data-streaming/data_lib/jodis-0.4.1.jar,file:///root/kairenlo/data-
streaming/data_lib/google-api-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-api-services-bigquery-v2-rev347-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-appengine-1.22.0.jar,file:
///root/kairenlo/data-streaming/data_lib/api-common-1.1.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson2-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-http-client-jackson-1.22.0.jar,file
:///root/kairenlo/data-streaming/data_lib/guava-20.0.jar,file:///root/kairenlo/data-streaming/data_lib/threetenbp-1.3.3.jar,file:///root/kairenlo/data-streaming/data_lib/gax-1.8.1.jar,file:///root/kairenlo/data-streaming/data_lib/googl
e-auth-library-oauth2-http-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-auth-library-credentials-0.8.0.jar,file:///root/kairenlo/data-streaming/data_lib/json-20160810.jar,file:///root/kairenlo/data-streaming/data_lib/
google-http-client-1.22.0.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-bigquery-0.25.0-beta.jar,file:///root/kairenlo/data-streaming/data_lib/google-cloud-core-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/g
oogle-cloud-core-http-1.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/
kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0
-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBa
se-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/
kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-1
0_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar --driver-class-path /root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar --verbose --conf spark.ui.
port=5771 --conf spark.app.name=bq_report_mix --conf spark.driver.extraClassPath=guava-20.0.jar --conf spark.executor.extraClassPath=guava-20.0.jar --files /root/kairenlo/data-streaming/bq/key.json,/root/kairenlo/data-streaming/bq/mix.
conf /root/kairenlo/data-streaming/bq/data-streaming.jar mix.conf modules= bq_report_campaign, bq_report_publisher, bq_report_events, bq_report_postback,bq_dsp_clock,bq_dsp_cloak_stat  buration=120 kill=true
user=root
priority=1
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/root/kairenlo/data-streaming/bq/bq_report_mix.log
stdout_logfile_maxbytes=100MB
stdout_logfile_backups=10
