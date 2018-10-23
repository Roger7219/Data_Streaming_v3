### yarn-client
spark-submit \
--name adx \
--class com.mobikok.ssp.data.streaming.App \
--master yarn-client \
--driver-memory 3g \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 1 \
--queue queueC \
--jars file:///root/kairenlo/data-streaming/data_lib/SspEvent.jar,file:///root/kairenlo/data-streaming/data_lib/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/adx/protobuf-java-3.0.0.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.10.1.0.jar,hdfs:/libs/kafka-clients-0.10.1.0.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.conf.app.name=adx \
/root/kairenlo/data-streaming/adx/data-streaming.jar \
/root/kairenlo/data-streaming/adx/adx.conf


### yarn-cluster
spark-submit \
--name adx \
--class com.mobikok.ssp.data.streaming.OptimizedMixApp \
--master yarn-cluster \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 4 \
--num-executors 2 \
--queue default \
--jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/SspEvent.jar,file:///root/kairenlo/data-streaming/data_lib/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar \
--verbose \
--conf spark.ui.port=1112 \
--conf spark.app.name=adx \
--files /root/kairenlo/data-streaming/adx/adx.conf \
/root/kairenlo/data-streaming/adx/data-streaming.jar \
adx.conf kill=true


# for auto restart
spark-submit --name adx --class com.mobikok.ssp.data.streaming.OptimizedMixApp --master yarn-cluster --driver-memory 6g --executor-memory 6g --executor-cores 4 --num-executors 1 --queue default --jars file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/SspEvent.jar,file:///root/kairenlo/data-streaming/data_lib/protobuf-java-format-1.2.jar,file:///root/kairenlo/data-streaming/data_lib/metrics-core-2.2.0.jar,file:///root/kairenlo/data-streaming/data_lib/spark-streaming-kafka-0-10_2.11-2.1.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka_2.11-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/kafka-clients-0.10.2.1.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-api-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/twill-discovery-api-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/twill-zookeeper-0.6.0-incubating.jar,file:///root/kairenlo/data-streaming/data_lib/tephra-core-0.7.0.jar,file:///root/kairenlo/data-streaming/data_lib/phoenix-core-4.7.0-HBase-1.1.jar,file:///root/kairenlo/data-streaming/data_lib/config-1.3.1.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-server-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/mysql-connector-java-5.1.42.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-protocol-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-common-1.1.10.jar,file:///root/kairenlo/data-streaming/data_lib/hbase-client-1.1.10.jar,hdfs:/libs/spark-streaming-kafka-0-10_2.11-2.1.1.jar,hdfs:/libs/kafka_2.11-0.11.0.1.jar,hdfs:/libs/kafka-clients-0.11.0.1.jar,hdfs:/libs/config-1.3.1.jar --verbose --conf spark.ui.port=1112 --conf spark.app.name=adx --files /root/kairenlo/data-streaming/adx/adx.conf /root/kairenlo/data-streaming/adx/data-streaming.jar adx.conf kill=true


select
concat(CASE event_type
                    WHEN 'REQUEST'    THEN request.trade.dsp_id
                    WHEN 'SEND'       THEN send.trade.dsp_id
                    WHEN 'WINNOTICE'  THEN win_notice.trade.dsp_id
                    WHEN 'IMPRESSION' THEN impression.trade.dsp_id
                    WHEN 'CLICK'      THEN click.trade.dsp_id
                    END,
'^', CASE event_type WHEN 'REQUEST'    THEN if(request.trade.status = 1, 1, 0) ELSE 0 END ,
'^',
event_key
)
from adx_ssp_dwi where b_date = "2018-04-01"



select count(1), count(distinct
concat(CASE event_type
                    WHEN 'REQUEST'    THEN request.trade.dsp_id
                    WHEN 'SEND'       THEN send.trade.dsp_id
                    WHEN 'WINNOTICE'  THEN win_notice.trade.dsp_id
                    WHEN 'IMPRESSION' THEN impression.trade.dsp_id
                    WHEN 'CLICK'      THEN click.trade.dsp_id
                    END,
'^', CASE event_type WHEN 'REQUEST'    THEN if(request.trade.status = 1, 1, 0) ELSE 0 END ,
'^',
event_key
))
from adx_dsp_dwi where b_date = "2018-03-20"


select count(1), repeated
from adx_dsp_dwi where b_date = "2018-03-20"
group by repeated


select count(1), count(distinct concat( event_key,
'^', from_unixtime(unix_timestamp(from_unixtime(timestamp/1000, 'yyyy-MM-dd HH:mm:ss')), 'yyyy-MM-dd HH:00:00')
)
)
from adx_ssp_dwi where b_date = "2018-04-01";



select count(1), count(distinct event_key)
from adx_ssp_dwi where b_date = "2018-04-01";

select count(1), repeated
from adx_ssp_dwi where b_date = "2018-04-01"
group by repeated;