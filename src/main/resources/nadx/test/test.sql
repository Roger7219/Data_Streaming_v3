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
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list node195:6667 --topic test0_v4

{"id":551, "name":"1", "a":10, "b":10,"timestamp":1564072052}
{"id":551, "name":"1", "a":2, "b":2,"timestamp":1564072052}
{"id":551, "name":"1", "a":2, "b":2,"timestamp":1563154081}

{"id":551, "name":"1", "a":1, "b":1,"timestamp":1563154081}
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


CREATE TABLE nadx_scraper_bundle_dwi(
 repeats int,
 rowkey string,

 timestamp bigint,
 bundle string

)PARTITIONED BY (repeated string, l_time string, b_date string, b_time string, b_version string)
STORED AS ORC;



SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND app_or_site_id IN (
    SELECT app_or_site_id
    FROM (
      SELECT app_or_site_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id
    ) t0
    WHERE supply_request_count > 1000
--  643条
  )
GROUP BY b_time
ORDER BY b_time DESC
--  2019-07-29 14:00:00     3621060


select count(1) from(
SELECT app_or_site_id
FROM (
  SELECT app_or_site_id, SUM(supply_request_count) AS supply_request_count
  FROM nadx_overall_dwr_v9
  WHERE b_time = '2019-07-29 14:00:00'
  GROUP BY app_or_site_id
) t0
-- WHERE supply_request_count > 1000
)
-- 96636条



SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain) IN (
    SELECT app_or_site_id, bundle_or_domain
    FROM (
      SELECT app_or_site_id, bundle_or_domain, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain
    ) t0
    WHERE supply_request_count > 1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     3607365


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     179210

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 500
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     274376


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 100
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     636152


select count(1) from(
  SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 100
);
-- 5799


select count(1) from(
  SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
);
-- 416486



SELECT b_time, sum(impression_revenue)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 100
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     6.933389193756006


SELECT b_time, sum(impression_revenue)
FROM nadx_overall_dwr_v9
WHERE b_time = '2019-07-29 14:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v9
      WHERE b_time = '2019-07-29 14:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count <= 100
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-29 14:00:00     6.681150958426998


SELECT  sum(impression_revenue) FROM nadx_overall_dwr_v9 WHERE b_time = '2019-07-29 14:00:00';
-- 13.828012920477942


select b_time, count(1) from nadx_overall_dwr_v12 where b_date='2019-07-31' group by b_time order by b_time desc;
-- 2019-07-31 17:00:00     935688
-- 2019-07-31 16:00:00     1196745

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 18:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 18:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 200
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 18:00:00     2707935


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 19:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 19:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count < 200
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 18:00:00     374233


select count(1) from(
  select count(1),app_or_site_id, bundle_or_domain, publisher_id as cc
  from nadx_overall_dwr_v12__pub_app_bun__counter
  where b_time = '2019-07-31 17:00:00'
  group by app_or_site_id, bundle_or_domain, publisher_id
  order by 1 desc
)where cc = 1



select count(1),app_or_site_id, bundle_or_domain, publisher_id as cc
from nadx_overall_dwr_v12__pub_app_bun__counter
where b_time = '2019-07-31 18:00:00'
group by app_or_site_id, bundle_or_domain, publisher_id
order by 1 desc
limit 4

--  1422333342      1422333342      91192

select *
from nadx_overall_dwr_v12__pub_app_bun__counter
where b_time = '2019-07-31 17:00:00' and app_or_site_id='1422333342' and bundle_or_domain ='1422333342' and publisher_Id= '91192'




select sum(supply_request_count)
from nadx_overall_dwr_v12
where b_time='2019-07-31 19:00:00'
group by app_or_site_id, bundle_or_domain, publisher_id



SELECT *
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 19:00:00'
AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
  SELECT app_or_site_id, bundle_or_domain,publisher_id
  FROM (
    SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
    FROM nadx_overall_dwr_v12
    WHERE b_time = '2019-07-31 19:00:00'
    GROUP BY app_or_site_id,bundle_or_domain,publisher_id
  ) t0
  WHERE supply_request_count < 200
)
limit 1


-- publisher_id                       nCumH
-- raterType                          NULL
-- raterId                            NULL
-- adomain                            NULL
-- crid                               NULL
-- bidfloor                           0.0
-- rater_type                         NULL
-- rater_id                           NULL
-- media_type                         App
-- app_or_site_id                     QAEgKNVLRhsdLoYt8Cm4sKShYH
-- bundle_or_domain                   387301602


select *
from nadx_overall_dwr_v12__pub_app_bun__counter
where
  app_or_site_id = 'QAEgKNVLRhsdLoYt8Cm4sKShYH'
  and bundle_or_domain = '387301602'
  and publisher_id = 'nCumH'
-- 436


SELECT *
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 19:00:00'
  and app_or_site_id = 'QAEgKNVLRhsdLoYt8Cm4sKShYH'
  and bundle_or_domain = '387301602'
  and publisher_id = 'nCumH'
limit 10


select count(1) from nadx_overall_dwr_v12

select count(distinct(supply_bd_id)) from nadx_overall_dwr_v12;
select count(distinct(supply_am_id)) from nadx_overall_dwr_v12 ;
select count(distinct (supply_id)) from nadx_overall_dwr_v12;
select count(distinct supply_protocol) from nadx_overall_dwr_v12;
select count(distinct ad_format) from nadx_overall_dwr_v12;
select count(distinct site_app_id) from nadx_overall_dwr_v12;
select count(distinct placement_id) from nadx_overall_dwr_v12;
select count(distinct position) from nadx_overall_dwr_v12 ;
select count(distinct supply_bd_id) from nadx_overall_dwr_v12;
select count(distinct supply_am_id) from nadx_overall_dwr_v12;
select count(distinct supply_id) from nadx_overall_dwr_v12 ;
select count(distinct supply_protocol) from nadx_overall_dwr_v12;
select count(distinct request_flag) from nadx_overall_dwr_v12  ;
select count(distinct ad_format) from nadx_overall_dwr_v12   ;
select count(distinct placement_id) from nadx_overall_dwr_v12    ;
select count(distinct position) from nadx_overall_dwr_v12    ``;
select count(distinct country) from nadx_overall_dwr_v12   ;
select count(distinct state) from nadx_overall_dwr_v12   ;
select count(distinct city) from nadx_overall_dwr_v12   ;
select count(distinct carrier) from nadx_overall_dwr_v12   ;
select count(distinct os) from nadx_overall_dwr_v12   ;
select count(distinct os_version) from nadx_overall_dwr_v12   ;
select count(distinct device_type) from nadx_overall_dwr_v12   ;
select count(distinct device_brand) from nadx_overall_dwr_v12   ;
select count(distinct device_model) from nadx_overall_dwr_v12   ;
select count(distinct age) from nadx_overall_dwr_v12   ;
select count(distinct gender) from nadx_overall_dwr_v12   ;
select count(distinct cost_currency) from nadx_overall_dwr_v12   ;
select count(distinct demand_bd_id) from nadx_overall_dwr_v12   ;
select count(distinct demand_am_id) from nadx_overall_dwr_v12   ;
select count(distinct demand_id) from nadx_overall_dwr_v12   ;
select count(distinct demand_protocol) from nadx_overall_dwr_v12   ;
select count(distinct revenue_currency) from nadx_overall_dwr_v12   ;
select count(distinct demand_seat_id) from nadx_overall_dwr_v12   ;
select count(distinct demand_campaign_id) from nadx_overall_dwr_v12   ;
select count(distinct target_site_app_id) from nadx_overall_dwr_v12   ;
select count(distinct bid_price_model) from nadx_overall_dwr_v12   ;
select count(distinct traffic_type) from nadx_overall_dwr_v12   ;
select count(distinct currency) from nadx_overall_dwr_v12   ;
select count(distinct bundle) from nadx_overall_dwr_v12    ;
select count(distinct size) from nadx_overall_dwr_v12   ;
select count(distinct tips) from nadx_overall_dwr_v12   ;
select count(distinct node) from nadx_overall_dwr_v12   ;
select count(distinct tip_type) from nadx_overall_dwr_v12   ;
select count(distinct tip_desc) from nadx_overall_dwr_v12   ;
select count(distinct ssp_token) from nadx_overall_dwr_v12   ;
select count(distinct rtb_version) from nadx_overall_dwr_v12   ;
select count(distinct demand_using_time) from nadx_overall_dwr_v12   ;
select count(distinct adx_using_time) from nadx_overall_dwr_v12   ;
select count(distinct site_domain) from nadx_overall_dwr_v12   ;
select count(distinct publisher_id) from nadx_overall_dwr_v12   ;
select count(distinct rater_type) from nadx_overall_dwr_v12   ;
select count(distinct rater_id) from nadx_overall_dwr_v12   ;
select count(distinct raterType) from nadx_overall_dwr_v12   ;
select count(distinct raterId) from nadx_overall_dwr_v12   ;
select count(distinct adomain) from nadx_overall_dwr_v12   ;
select count(distinct crid) from nadx_overall_dwr_v12   ;
select count(distinct media_type) from nadx_overall_dwr_v12   ;
select count(distinct app_or_site_id) from nadx_overall_dwr_v12   ;
select count(distinct bundle_or_domain) from nadx_overall_dwr_v12   ;


select sum(nadx_overall_dwr_v12__pub_app_bun__counter) nadx_overall_dwr_v12__pub_app_bun__counter


select sum(supply_request_count), app_or_site_id,bundle_or_domain,publisher_id
from nadx_overall_dwr_v12__pub_app_bun__counter
where l_time='2019-07-31 00:00:00'
group by app_or_site_id,bundle_or_domain,publisher_id
order by 1 desc
limit 10

select sum(supply_request_count) from nadx_overall_dwr_v12 where app_or_site_id ='120201995' and bundle_or_domain = '1207472156' and publisher_id ='1100043054';
select sum(supply_request_count) from nadx_overall_dwr_v12 where app_or_site_id ='dec5102ba463' and bundle_or_domain = 'stepcounter.pedometer.steps.steptracker' and publisher_id ='35742168315211';

select * from nadx_overall_dwr_v12 where app_or_site_id ='vDxfBRcOauGAksreUs' and bundle_or_domain = 'com.linsekog.prince.run3.temple' and publisher_id ='m1bZFEjXcb';

alter table nadx_overall_traffic_dwi_v12 add column ()
select * from nadx_overall_traffic_dwi_v12
where app_or_site_id ='vDxfBRcOauGAksreUs' and bundle_or_domain = 'com.linsekog.prince.run3.temple' and publisher_id ='m1bZFEjXcb' limit 10;



truncate table nadx_overall_traffic_dwi_v12;
truncate table nadx_overall_dwr_v12__pub_app_bun__counter;
truncate table nadx_overall_dwr_v12;
truncate table nadx_overall_performance_matched_dwi_v12;


select b_time,sum(supply_request_count) from nadx_overall_traffic_dwi_v12 group by b_time order by b_time desc;
-- 2019-07-31 21:00:00     5223551
-- 2019-07-31 20:00:00     3327299
-- 2019-07-25 22:00:00     NULL
-- 2019-07-25 21:00:00     NULL
-- 2019-07-19 22:00:00     NULL
select b_time,sum(supply_request_count) from nadx_overall_dwr_v12 group by b_time order by b_time desc;
-- __HIVE_DEFAULT_PARTITION__      0
-- 2019-07-31 21:00:00     5223551
-- 2019-07-31 20:00:00     3327299
-- 2019-07-25 22:00:00     NULL
-- 2019-07-25 21:00:00     NULL
-- 2019-07-19 22:00:00     NULL

select b_time, count(1) from nadx_overall_traffic_dwi_v12 group by b_time;

select b_time, count(1) from nadx_overall_dwr_v12 group by b_time;
select count(1) from nadx_overall_dwr_v12__pub_app_bun__counter;

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count < 200
  )
GROUP BY b_time
ORDER BY b_time DESC


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count < 200
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 21:00:00     190242

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 200
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 21:00:00     1724784

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 500
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 21:00:00     1394900


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 100
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 21:00:00     1835319

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-07-31 21:00:00     1086012



select sum(supply_request_count), app_or_site_id,bundle_or_domain,publisher_id
from nadx_overall_dwr_v12__pub_app_bun__counter
where l_time='2019-07-31 00:00:00' and b_time ='2019-07-31 21:00:00'
group by app_or_site_id,bundle_or_domain,publisher_id
order by 1 asc
limit 10

select b_time, sum(supply_request_count)
from nadx_overall_dwr_v12__pub_app_bun__counter
group by b_time
order by b_time desc
-- __HIVE_DEFAULT_PARTITION__      0
-- 2019-07-31 21:00:00     5223551
-- 2019-07-31 20:00:00     3327299
-- 2019-07-25 22:00:00     NULL
-- 2019-07-25 21:00:00     NULL
-- 2019-07-19 22:00:00     NULL

select sum(supply_request_count),count(1) from nadx_overall_traffic_dwi_v12
 where app_or_site_id='30f18d60bbe1' and bundle_or_domain='com.wibw.android.weather'
 and publisher_id = '189_1039_dEtTBb6I'

select * from nadx_overall_traffic_dwi_v12
 where app_or_site_id='30f18d60bbe1' and bundle_or_domain='com.wibw.android.weather'
 and publisher_id = '189_1039_dEtTBb6I'
--  4条记录

select * from nadx_overall_dwr_v12 where  app_or_site_id='30f18d60bbe1' and bundle_or_domain='com.wibw.android.weather'
 and publisher_id = '189_1039_dEtTBb6I'




SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 100
  )
GROUP BY b_time
ORDER BY b_time DESC
-- b_time    2019-07-31 21:00:00
-- count(1)  1915026

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count > 1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- b_time    2019-07-31 21:00:00
-- count(1)  1224668


select count(distinct (app_or_site_id, bundle_or_domain,publisher_id))
from nadx_overall_dwr_v12__pub_app_bun__counter
where b_time = '2019-07-31 21:00:00' and  l_time <>'0001-01-01 00:00:00'
-- 193786
select count(1)
from nadx_overall_dwr_v12__pub_app_bun__counter
where b_time = '2019-07-31 21:00:00' and l_time <>'0001-01-01 00:00:00'
-- 193786



SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
GROUP BY b_time
ORDER BY b_time DESC
-- b_time    2019-07-31 21:00:00
-- count(1)  2334821

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
GROUP BY b_time
ORDER BY b_time DESC



SELECT sum(supply_request_count) from(
  FROM (
    SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
    FROM nadx_overall_dwr_v12__pub_app_bun__counter
    WHERE b_time = '2019-07-31 21:00:00'
    GROUP BY app_or_site_id,bundle_or_domain,publisher_id
  ) t0
  WHERE supply_request_count < 200
)
-- 1298909

SELECT sum(supply_request_count) from(
  FROM (
    SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
    FROM nadx_overall_dwr_v12__pub_app_bun__counter
    WHERE b_time = '2019-07-31 21:00:00'
    GROUP BY app_or_site_id,bundle_or_domain,publisher_id
  ) t0
  WHERE supply_request_count > 0
)
-- 5223551


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 200
  )
GROUP BY b_time
ORDER BY b_time DESC
-- b_time    2019-07-31 21:00:00
-- count(1)  1915026

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- | 2019-07-31 21:00:00  | 1226578   |


SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 2000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- | 2019-07-31 21:00:00  | 851541    |


SELECT b_time, sum(supply_request_count)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 2000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2217009

SELECT b_time, sum(supply_request_count)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-07-31 21:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-07-31 21:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 0
  )
GROUP BY b_time
ORDER BY b_time DESC


SELECT b_time, COUNT(1) from nadx_overall_dwr_v12__pub_app_bun__counter group by b_time order by b_time;
-- 2019-04-02 07:00:00     1
-- 2019-04-03 00:00:00     1
-- 2019-06-26 11:00:00     1
-- 2019-07-17 11:00:00     1
-- 2019-07-22 05:00:00     1
-- 2019-07-24 00:00:00     2142
-- 2019-07-25 20:00:00     1
-- 2019-07-27 13:00:00     1
-- 2019-07-29 00:00:00     1323
-- 2019-07-29 20:00:00     1
-- 2019-08-01 00:00:00     3234
-- 2019-08-01 01:00:00     240701
-- 2019-08-01 02:00:00     212697
-- 2019-08-01 03:00:00     210811
-- 2019-08-01 04:00:00     214039
-- 2019-08-01 05:00:00     221654
-- 2019-08-01 06:00:00     232956
-- 2019-08-01 07:00:00     212472
-- 2019-08-01 08:00:00     183875
-- __HIVE_DEFAULT_PARTITION__

SELECT b_time, COUNT(1) from nadx_overall_dwr_v12 group by b_time  order by b_time;
-- 2019-04-02 07:00:00     1
-- 2019-04-03 00:00:00     1
-- 2019-06-26 11:00:00     1
-- 2019-07-17 11:00:00     1
-- 2019-07-22 05:00:00     1
-- 2019-07-24 00:00:00     3833
-- 2019-07-25 20:00:00     1
-- 2019-07-27 13:00:00     1
-- 2019-07-29 00:00:00     2813
-- 2019-07-29 20:00:00     1
-- 2019-08-01 00:00:00     18017
-- 2019-08-01 01:00:00     1202393
-- 2019-08-01 02:00:00     1456511
-- 2019-08-01 03:00:00     1364947
-- 2019-08-01 04:00:00     1388018
-- 2019-08-01 05:00:00     1356751
-- 2019-08-01 06:00:00     1284906
-- 2019-08-01 07:00:00     1250041
-- 2019-08-01 08:00:00     913736
-- __HIVE_DEFAULT_PARTITION__      1
select b_time, count(1) from nadx_overall_traffic_dwi_v12 group by b_time order by b_time;
-- 2019-04-02 07:00:00     1
-- 2019-04-03 00:00:00     1
-- 2019-06-26 11:00:00     1
-- 2019-07-17 11:00:00     1
-- 2019-07-22 05:00:00     1
-- 2019-07-24 00:00:00     10576
-- 2019-07-25 20:00:00     5
-- 2019-07-27 13:00:00     1
-- 2019-07-29 00:00:00     6060
-- 2019-07-29 20:00:00     1
-- 2019-08-01 00:00:00     1
-- 2019-08-01 01:00:00     42482912
-- 2019-08-01 02:00:00     49311373
-- 2019-08-01 03:00:00     51230730
-- 2019-08-01 04:00:00     50975291
-- 2019-08-01 05:00:00     51140228
-- 2019-08-01 06:00:00     51538408
-- 2019-08-01 07:00:00     49506374
-- 2019-08-01 08:00:00     29588546




select b_time, sum(impression_revenue) from nadx_overall_dwr_v12__pub_app_bun__counter group by b_time
-- 2019-07-31 23:00:00     492065
-- 2019-08-01 00:00:00     705768
-- __HIVE_DEFAULT_PARTITION__      3
-- 2019-07-21 05:00:00     1



SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-08-01 06:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-08-01 06:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 20000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-08-01 06:00:00     434066



select count(1) from nadx_overall_dwr_v12
WHERE b_time = '2019-08-01 06:00:00'
and app_or_site_id is null
and bundle_or_domain is null
and publisher_id is null
-- 850840


select sum(impression_count) from nadx_overall_dwr_v12
WHERE b_time = '2019-08-01 06:00:00'
and app_or_site_id is null
and bundle_or_domain is null
and publisher_id is null
-- 8953

SELECT b_time, sum(impression_count) from nadx_overall_dwr_v12 group by b_time  order by b_time;
-- 2019-04-02 07:00:00     0
-- 2019-04-03 00:00:00     0
-- 2019-06-26 11:00:00     0
-- 2019-07-17 11:00:00     0
-- 2019-07-22 05:00:00     0
-- 2019-07-24 00:00:00     0
-- 2019-07-25 20:00:00     0
-- 2019-07-27 13:00:00     0
-- 2019-07-29 00:00:00     0
-- 2019-07-29 20:00:00     0
-- 2019-08-01 00:00:00     9203
-- 2019-08-01 01:00:00     9005
-- 2019-08-01 02:00:00     10737
-- 2019-08-01 03:00:00     10921
-- 2019-08-01 04:00:00     11592
-- 2019-08-01 05:00:00     9390
-- 2019-08-01 06:00:00     8953
-- 2019-08-01 07:00:00     8615
-- 2019-08-01 08:00:00     6387
-- __HIVE_DEFAULT_PARTITION__      1
select b_time, sum(impression_count) from nadx_overall_traffic_dwi_v12 group by b_time order by b_time;

select b_time, sum(impression_count) from nadx_overall_dwr_v12__pub_app_bun__counter group by b_time order by b_time;
-- 2019-04-02 07:00:00     0
-- 2019-04-03 00:00:00     0
-- 2019-06-26 11:00:00     0
-- 2019-07-17 11:00:00     0
-- 2019-07-22 05:00:00     0
-- 2019-07-24 00:00:00     0
-- 2019-07-25 20:00:00     0
-- 2019-07-27 13:00:00     0
-- 2019-07-29 00:00:00     0
-- 2019-07-29 20:00:00     0
-- 2019-08-01 00:00:00     9203
-- 2019-08-01 01:00:00     9005
-- 2019-08-01 02:00:00     10737
-- 2019-08-01 03:00:00     10921
-- 2019-08-01 04:00:00     11592
-- 2019-08-01 05:00:00     9390
-- 2019-08-01 06:00:00     8953
-- 2019-08-01 07:00:00     8615
-- 2019-08-01 08:00:00     6728
-- __HIVE_DEFAULT_PARTITION__      1





 SELECT b_time, sum(supply_request_count)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-08-01 06:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-08-01 06:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 20000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-08-01 06:00:00     4346699

select sum(supply_request_count)
from nadx_overall_dwr_v12
where b_time = '2019-08-01 06:00:00'
and app_or_site_id is not null
and bundle_or_domain is not null
and publisher_id is not null;
-- 4346699

select count(1)
from nadx_overall_dwr_v12
where b_time = '2019-08-01 06:00:00'
and app_or_site_id   is not null
and bundle_or_domain is not null
and publisher_id     is not null;
-- 434066

select count(1)
from nadx_overall_dwr_v12
where b_time = '2019-08-01 06:00:00'
and app_or_site_id   is  null
and bundle_or_domain is  null
and publisher_id     is  null;
-- 850840

select count(1)
from nadx_overall_dwr_v12
where b_time = '2019-08-01 06:00:00';
-- 1284906

SELECT b_time, COUNT(1)
FROM nadx_overall_dwr_v12
WHERE b_time = '2019-08-01 06:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v12__pub_app_bun__counter
      WHERE b_time = '2019-08-01 06:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count >= 20000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- 2019-08-01 06:00:00     434066




select count(distinct (
supply_bd_id,
supply_am_id,
supply_id,
supply_protocol,
request_flag,
ad_format,
site_app_id,
placement_id,
position,
country,
state,
city,
carrier,
os,
os_version,
device_type,
device_brand,
device_model,
age,
gender,
cost_currency,
demand_bd_id,
demand_am_id,
demand_id,
demand_protocol,
revenue_currency,
demand_seat_id,
demand_campaign_id,
target_site_app_id,
bid_price_model,
traffic_type,
currency,
bundle,
size,
tips,
node,
tip_type,
tip_desc,
ssp_token,
rtb_version,
demand_using_time,
adx_using_time,
site_domain,
publisher_id,
rater_type,
rater_id,
raterType,
raterId,
adomain,
crid,
media_type,
app_or_site_id,
bundle_or_domain
))
from nadx_overall_dwr_v12
where b_time = '2019-08-01 06:00:00'
and app_or_site_id   is  null
and bundle_or_domain is  null
and publisher_id     is  null
-- 850840



select count(distinct(supply_bd_id)) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct(supply_am_id)) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null ;
select count(distinct (supply_id)) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct supply_protocol) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct ad_format) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct site_app_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct placement_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct position) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null ;
select count(distinct supply_bd_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct supply_am_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct supply_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null ;
select count(distinct supply_protocol) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;
select count(distinct request_flag) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null  ;
select count(distinct ad_format) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct placement_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null    ;
select count(distinct position) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null    ;
select count(distinct country) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct state) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct city) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct carrier) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct os) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct os_version) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct device_type) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct device_brand) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct device_model) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct age) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct gender) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct cost_currency) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_bd_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_am_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_protocol) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct revenue_currency) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_seat_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_campaign_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct target_site_app_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct bid_price_model) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct traffic_type) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct currency) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct bundle) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null    ;
select count(distinct size) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct tips) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct node) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct tip_type) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct tip_desc) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct ssp_token) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct rtb_version) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct demand_using_time) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct adx_using_time) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct site_domain) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct publisher_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct rater_type) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct rater_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct raterType) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct raterId) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct adomain) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct crid) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct media_type) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct app_or_site_id) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null   ;
select count(distinct bundle_or_domain) from nadx_overall_dwr_v12 where b_time = '2019-08-01 06:00:00' and app_or_site_id   is  null and bundle_or_domain is  null and publisher_id     is  null;




select b_time,count(1) from nadx_overall_dwr_v12 group  by b_time

alter table nadx_overall_dwr_v12__pub_app_bun__counter_m_dwr_traffic_v12_backup_progressing_20190801_112004_969__5 rename to nadx_overall_dwr_v12__pub_app_bun__counter_m_dwr_traffic_v12_backup_completed_20190801_112004_969__5
alter table nadx_overall_dwr_v12__pub_app_bun__counter_m_dwr_traffic_v12_backup_progressing_20190801_112004_969__5 rename to nadx_overall_dwr_v12__pub_app_bun__counter_m_dwr_traffic_v12_backup_completed_20190801_112004_969__5


SELECT b_time, COUNT(1), sum(impression_count)
FROM nadx_overall_dwr_v13
WHERE b_time = '2019-08-01 11:00:00'
  AND (app_or_site_id, bundle_or_domain, publisher_id) IN (
    SELECT app_or_site_id, bundle_or_domain,publisher_id
    FROM (
      SELECT app_or_site_id, bundle_or_domain,publisher_id, SUM(supply_request_count) AS supply_request_count
      FROM nadx_overall_dwr_v13__pub_app_bun__counter
      WHERE b_time = '2019-08-01 11:00:00'
      GROUP BY app_or_site_id,bundle_or_domain,publisher_id
    ) t0
    WHERE supply_request_count <1000
  )
GROUP BY b_time
ORDER BY b_time DESC
-- <1000: 2019-08-01 11:00:00     635161  1757
-- 1000: 2019-08-01 11:00:00     1506005 4071
-- 0: 2019-08-01 11:00:00     2141164 5827

SELECT b_time, COUNT(1), sum(impression_count)
FROM nadx_overall_dwr_v13
WHERE b_time = '2019-08-01 11:00:00'
GROUP BY b_time
ORDER BY b_time DESC
-- 2166035



select b_time,count(1) from nadx_overall_dwr_v15 group by b_time;
select b_time,count(1) from nadx_overall_dwr_v15__counter group by b_time;


select b_time, count(1) from nadx_overall_dwr_v16 group by b_time
-- 2019-08-01 12:00:00     32115
select b_time, count(1) from nadx_overall_dwr_v16__counter group by b_time
-- 2019-08-01 12:00:00     549678


select count(1)
from nadx_overall_dwr_v16
where b_time = '2019-08-01 13:00:00'
and app_or_site_id   is not null
and bundle_or_domain is not null
and publisher_id     is not null;


select count(1)
from nadx_overall_dwr_v16
where b_time = '2019-08-01 13:00:00'
and app_or_site_id   is  null
and bundle_or_domain is  null
and publisher_id     is  null;
-- 34735

select count(1)
 from(
  select sum(supply_request_count) supply_request_count
  from nadx_overall_dwr_v16
  group by supply_bd_id,
  supply_am_id,
  supply_id,
  supply_protocol,
  request_flag,
  ad_format,
  site_app_id,
  placement_id,
  position,
  country,
  state,
  city,
  carrier,
  os,
  os_version,
  device_type,
  device_brand,
  device_model,
  age,
  gender,
  cost_currency,
  demand_bd_id,
  demand_am_id,
  demand_id,
  demand_protocol,
  revenue_currency,
  demand_seat_id,
  demand_campaign_id,
  target_site_app_id,
  bid_price_model,
  traffic_type,
  currency,
  bundle,
  size,
  tips,
  node,
  tip_type,
  tip_desc,
  ssp_token,
  rtb_version,
  demand_using_time,
  adx_using_time,
  site_domain,
  publisher_id,
  rater_type,
  rater_id,
  raterType,
  raterId,
  adomain,
  crid,
  media_type,
  app_or_site_id,
  bundle_or_domain
)t
where supply_request_count>10


-- tip_type,supply_id,demand_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain
select b_time, count(1) from nadx_overall_dwr_v18 where b_date= '2019-08-01' group by b_time order by b_time;
-- 2019-08-01 14:00:00     332845
select b_time, count(1) from nadx_overall_dwr_v18_1 where b_date= '2019-08-01' group by b_time order by b_time;
-- 1165610

 select b_time, count(1) from nadx_overall_dwr_v18
 where b_date= '2019-08-01'
 group by b_time order by b_time;



select
  b_time,
  count(1),
  sum(impression_count),
  SUM(impression_revenue)
from(
  SELECT
    b_time,supply_id,demand_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain,
    SUM(supply_request_count) AS supply_request_count,
    sum(impression_count)     AS impression_count,
    SUM(impression_revenue)   AS impression_revenue
  FROM nadx_overall_dwr_v18_1
  WHERE b_date = '2019-08-01'
  GROUP BY b_time,supply_id,demand_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain
)
where supply_request_count>5
group by b_time
order by b_time
-- 2019-08-01 14:00:00     8922    14255   8.614268432992997
-- 2019-08-01 15:00:00     366939  14255   7.930238500642
-- 2019-08-01 16:00:00     141272  2953    1.6510407740640003

select
  b_time,
  count(1),
  sum(impression_count),
  SUM(impression_revenue)
from(
  SELECT
    b_time,tip_type,supply_id,demand_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain,
    SUM(supply_request_count) AS supply_request_count,
    sum(impression_count)     AS impression_count,
    SUM(impression_revenue)   AS impression_revenue
  FROM nadx_overall_dwr_v18_1
  WHERE b_date = '2019-08-01'
  GROUP BY b_time,supply_id,demand_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain
)
-- where supply_request_count>=0
group by b_time
order by b_time



select b_time,count(1)
from (
select b_time
from nadx_overall_dwr_v18_1
group by node,media_type,supply_id,publisher_id,adomain,crid,app_or_site_id,bundle_or_domain, b_time
)
group by b_time
order by b_time desc;



select count(1)
from (
select bundle_or_domain
from nadx_overall_dwr_v18_1 where b_time >= '2019-08-01 18:00:00'
group by bundle_or_domain
)

select sum(supply_request_count) from nadx_overall_dwr_v18_1 where b_time = '2019-08-01 18:00:00' and tip_type=10000

select count(1), b_time from nadx_overall_dwr_v18_1 group by b_time order by b_time
-- 16666   2019-08-01 14:00:00
-- 1837400 2019-08-01 15:00:00
-- 1878380 2019-08-01 16:00:00
-- 1231656 2019-08-01 17:00:00
-- 1763037 2019-08-01 18:00:00
-- 5693443 2019-08-01 19:00:00
-- 2814168 2019-08-01 20:00:00
-- 1464223 2019-08-01 21:00:00
-- 1490612 2019-08-01 22:00:00
-- 1736306 2019-08-01 23:00:00
-- 1419741 2019-08-02 00:00:00
-- 1360647 2019-08-02 01:00:00
-- 1351920 2019-08-02 02:00:00
-- 1283126 2019-08-02 03:00:00
-- 1324135 2019-08-02 04:00:00
-- 1346281 2019-08-02 05:00:00
-- 1386133 2019-08-02 06:00:00
-- 1269440 2019-08-02 07:00:00
-- 1423375 2019-08-02 08:00:00
-- 1008508 2019-08-02 09:00:00

select count(1), b_time from nadx_overall_dwr_v18 group by b_time order by b_time
-- 5109    2019-07-31 15:00:00
-- 2715    2019-07-31 18:00:00
-- 1       2019-08-01 00:00:00
-- 1       2019-08-01 10:00:00
-- 1       2019-08-01 12:00:00
-- 32157   2019-08-01 14:00:00
-- 1109183 2019-08-01 15:00:00
-- 1035339 2019-08-01 16:00:00
-- 699865  2019-08-01 17:00:00
-- 1066793 2019-08-01 18:00:00
-- 1101728 2019-08-01 19:00:00
-- 1028194 2019-08-01 20:00:00
-- 1306124 2019-08-01 21:00:00
-- 1429880 2019-08-01 22:00:00
-- 1748977 2019-08-01 23:00:00
-- 1323952 2019-08-02 00:00:00
-- 1393611 2019-08-02 01:00:00
-- 1347583 2019-08-02 02:00:00
-- 1378617 2019-08-02 03:00:00
-- 1370578 2019-08-02 04:00:00
-- 1293953 2019-08-02 05:00:00
-- 1305635 2019-08-02 06:00:00
-- 1309425 2019-08-02 07:00:00
-- 1338132 2019-08-02 08:00:00
-- 1086711 2019-08-02 09:00:00


select b_time, sum(supply_request_count) from nadx_overall_dwr_v20 group by b_time order by b_time desc;
select b_time, sum(supply_request_count) from nadx_overall_dwr_v20_1 group by b_time order by b_time desc;
select b_time, sum(supply_request_count) from nadx_overall_dwr_v20_2 where tip_type in(10000, 20000, 30000, 40000, 41000, 42000, 50000, 60000, 70000, 80000, 90000) group by b_time order by b_time desc;
select b_time, sum(supply_request_count) from nadx_overall_dwr_v20_3 group by b_time order by b_time desc;



select b_time, sum(supply_request_count) from nadx_overall_dwr_v18 where b_date>='2019-08-05' group by b_time order by b_time desc;
select b_time, sum(supply_request_count) from nadx_overall_dwr_v6 where b_date>='2019-08-05' and tip_type in (10000) group by b_time order by b_time desc;

select b_time, count(1) from nadx_overall_dwr_v18 where b_date>='2019-08-02' group by b_time order by b_time desc;
-- 2019-08-02 17:00:00     37632
-- 2019-08-02 16:00:00     45512
-- 2019-08-02 15:00:00     48078
-- 2019-08-02 14:00:00     59166
-- 2019-08-02 13:00:00     21396
select b_time, count(1) from nadx_overall_dwr_v18_1 where b_date>='2019-08-02' group by b_time order by b_time desc;
-- 2019-08-02 17:00:00     1638067
-- 2019-08-02 16:00:00     2333850
-- 2019-08-02 15:00:00     1725060
-- 2019-08-02 14:00:00     1556689
-- 2019-08-02 13:00:00     293475
select b_time, count(1) from nadx_overall_dwr_v18_2 group by b_time order by b_time desc;
-- __HIVE_DEFAULT_PARTITION__      2
-- 2019-08-02 17:00:00     20530
-- 2019-08-02 16:00:00     31916
-- 2019-08-02 15:00:00     33203
-- 2019-08-02 14:00:00     31089
-- 2019-08-02 13:00:00     6653
-- 2019-08-01 16:00:00     1
-- 2019-07-29 11:00:00     1
-- 2019-07-20 00:00:00     1
-- 2019-06-21 22:00:00     1
-- 2019-06-12 12:00:00     1
-- 2019-03-03 18:00:00     1
select b_time, count(1) from nadx_overall_dwr_v18_3 group by b_time order by b_time desc;
-- 2019-08-02 17:00:00     17045
-- 2019-08-02 16:00:00     21831
-- 2019-08-02 15:00:00     20429
-- 2019-08-02 14:00:00     21645
-- 2019-08-02 13:00:00     9027


select  b_time, count(distinct(rater_type,rater_id,supply_id,country,publisher_id,app_or_site_id,bundle_or_domain))
from nadx_overall_dwr_v18_1
where
group by b_time;


select  b_time, count(1)
from nadx_overall_dwr_v18_1
where
group by b_time;

-- 2019-08-02 14:00:00     1553411
-- 2019-08-02 13:00:00     293475
-- __HIVE_DEFAULT_PARTITION__      1
-- 2019-08-02 15:00:00     694926


-- 2019-08-02 14:00:00     1556689
-- 2019-08-02 13:00:00     293475
-- __HIVE_DEFAULT_PARTITION__      1
-- 2019-08-02 15:00:00     696617



select * from nadx_overall_dwr_v18_2 where supply_id = 99 and tip_desc is not null limit 1


select * from nadx_overall_dwr_v18_1 where supply_id = 99 and tip_desc is not null limit 1



--  0= {
--         select= "supply_bd_id, supply_am_id, supply_id, supply_protocol, ad_format, country, os, device_type, gender, cost_currency, demand_bd_id, demand_am_id, demand_id, demand_protocol, revenue_currency, bid_price_model, traffic_type, currency, size, ssp_token, rtb_version, media_type"
--         where =  "tip_type in(10000, 20000, 30000, 40000, 41000, 42000, 50000, 60000, 70000, 80000, 90000)"
--       }
--       // 流量调控依据
--       1= {
--         select= "rater_type,rater_id,supply_id,demand_id,country,publisher_id,app_or_site_id,bundle_or_domain"
--         where = "tip_type in(10000, 20000, 30000, 40000, 41000, 42000, 50000, 60000, 70000, 80000, 90000)"
--       }
--       // tip_type运营分析
--       2= {
--         select= "node,tip_type,tip_desc,supply_id,demand_id,demand_using_time,adx_using_time"
--       }
--       // bundle爬虫用
--       3= {
--         select= "media_type,bundle_or_domain"
--         where = "tip_type in(10000)"
--

select sum(supply_request_count) from nadx_overall_dwr_v6
where b_time ='2019-08-02 16:00:00' and tip_type in(10000)

select sum(supply_request_count) from nadx_overall_dwr_v18
where b_time ='2019-08-02 16:00:00'
-- 11672537
select sum(supply_request_count) from nadx_overall_dwr_v18_1
where b_time ='2019-08-02 16:00:00'
-- 11672537

select sum(supply_request_count) from nadx_overall_dwr_v18_2
where b_time ='2019-08-02 16:00:00' and tip_type in(10000)
-- 11672537

select sum(supply_request_count) from nadx_overall_dwr_v18_3
where b_time ='2019-08-02 16:00:00'
-- 11672537

-- 2019-08-02 13:00:00     851638
-- 2019-08-02 14:00:00     10264989
-- 2019-08-02 15:00:00     11784999
-- 2019-08-02 16:00:00     11664449
-- 2019-08-02 17:00:00     1397982

select b_time, sum(supply_request_count) from nadx_overall_dwr_v18_2
where tip_type in(10000)
group by b_time order by b_time;

-- 2019-03-03 18:00:00     NULL
-- 2019-06-12 12:00:00     NULL
-- 2019-06-21 22:00:00     NULL
-- 2019-08-01 16:00:00     NULL
-- 2019-08-02 13:00:00     1016209
-- 2019-08-02 14:00:00     12502048
-- 2019-08-02 15:00:00     13661989
-- 2019-08-02 16:00:00     13322549
-- 2019-08-02 17:00:00     1998873


ALTER TABLE $clickHouseTable DROP PARTITION ($partition)

