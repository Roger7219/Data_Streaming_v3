--hive
drop table if exists  ssp_test930_dwi ;
create table ssp_test930_dwi(
    repeats     INT,
    rowkey      STRING,
    appId       int
)
PARTITIONED BY (repeated STRING,l_time STRING, b_date string)
STORED AS ORC;

drop table if exists  ssp_test930_dwr ;
create table ssp_test930_dwr(
    appId       int,
    times       BIGINT
)
PARTITIONED BY (l_time STRING, b_date string)
STORED AS ORC;



