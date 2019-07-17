drop table test_dwr;
CREATE TABLE test_dwr(
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

drop table test_a_dwi;
CREATE TABLE test_a_dwi(
  repeats int,
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

drop table test_b_dwi;
CREATE TABLE test_b_dwi(
  repeats int,
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;
drop table nadx_log_table;
CREATE TABLE nadx_log_table(
 table_name  string,
 field_value string,
 count bigint
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING,field_name STRING)
STORED AS ORC;

select * from test_a_dwi;
select * from test_b_dwi;
select * from test_dwr;
select * from nadx_log_table;



select * from test_a_dwi_v2;
select * from test_b_dwi_v2;
select * from test_dwr_v2;
select * from nadx_log_table;
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list node195:6667 --topic test0

{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154081}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154082}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154083}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154084}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154085}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154086}
{"id":551, "name":"1", "a":9, "b":9,"timestamp":1563154087}
{"id":551, "name":"2", "a":9, "b":9,"timestamp":1563154088}
{"id":551, "name":"3", "a":9, "b":9,"timestamp":1563154089}
{"id":234, "name":"4", "a":9, "b":9,"timestamp":1563264210}
{"id":234, "name":"5", "a":9, "b":9,"timestamp":1563264210}
{"id":234, "name":"3", "a":9, "b":9,"timestamp":1563340084}
set hive.exec.dynamic.partition.mode=nonstrict

insert into table test_a_dwi
select
 0 as repeats,
 55 as id,
 "a" as name,
 8 as a,
3 as b,
 "Y",
 '0000-01-01 00:00:00',
 '2019=09-09',
 '2019=09-09 01:00:00',
 "0"
from dual;

/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper node187:2181/kafka --topic test0 --from-beginning


drop table test_dwr_v2;
CREATE TABLE test_dwr_v2(
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

drop table test_a_dwi_v2;
CREATE TABLE test_a_dwi_v2(
  repeats int,
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;

drop table test_b_dwi_v2;
CREATE TABLE test_b_dwi_v2(
  repeats int,
  id                           int,
  name                    string,
  a                    int,
  b                    int
)
PARTITIONED BY (repeated string, l_time STRING, b_date STRING, b_time STRING, b_version STRING)
STORED AS ORC;
drop table nadx_log_table;
CREATE TABLE nadx_log_table(
 table_name  string,
 field_value string,
 count bigint
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING,field_name STRING)
STORED AS ORC;