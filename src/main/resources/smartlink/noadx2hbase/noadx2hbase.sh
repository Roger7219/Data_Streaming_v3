spark-submit \
--name noadx2hbase \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 10g \
--executor-memory 3g \
--executor-cores 2 \
--num-executors 2 \
--queue default \
--jars \
file:///root/kairenlo/data-streaming/data_lib/scala-library-2.11.8.jar,\
file:///root/kairenlo/data-streaming/data_lib/scala-reflect-2.11.8.jar,\
file:///root/kairenlo//data-streaming/data_lib/datanucleus-api-jdo-3.2.6.jar,\
file:///root/kairenlo//data-streaming/data_lib/datanucleus-core-3.2.10.jar,\
file:///root/kairenlo//data-streaming/data_lib/datanucleus-rdbms-3.2.9.jar,\
file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,\
file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,\
file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,\
file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,\
file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,\
file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,\
file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,\
file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,\
file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,\
file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,\
file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,\
file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,\
file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,\
file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,\
file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar \
--verbose --conf spark.ui.port=2635 \
--conf spark.app.name=noadx2hbase \
--files /root/kairenlo/data-streaming/smartlink/noadx2hbase/noadx2hbase.conf \
/root/kairenlo/data-streaming/smartlink/noadx2hbase/data-streaming.jar \
noadx2hbase.conf kill=true


[program:noadx2hbase]
directory=/root/kairenlo/data-streaming/smartlink/noadx2hbase/
command=spark-submit --name noadx2hbase --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 10g --executor-memory 3g --executor-cores 2 --num-executors 2 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/scala-library-2.11.8.jar,file:///root/kairenlo/data-streaming/data_lib/scala-reflect-2.11.8.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-api-jdo-3.2.6.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-core-3.2.10.jar,file:///root/kairenlo//data-streaming/data_lib/datanucleus-rdbms-3.2.9.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar --verbose --conf spark.ui.port=2635 --conf spark.app.name=noadx2hbase --files /root/kairenlo/data-streaming/smartlink/noadx2hbase/noadx2hbase.conf /root/kairenlo/data-streaming/smartlink/noadx2hbase/data-streaming.jar noadx2hbase.conf kill=true
priority=1
numprocs=1
autostart=true
autorestart=true
startretries=9999
stopsignal=KILL
stopwaitsecs=10
redirect_stderr=true
stdout_logfile=/root/kairenlo/data-streaming/smartlink/noadx2hbase/log
stdout_logfile_maxbytes=100MB
stdout_logfile_backups=10





