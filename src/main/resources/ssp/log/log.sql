create table ssp_log_dwi(
    repeats     INT   , --default 0
    rowkey      STRING, --default ''
    appId      INT,
    createTime STRING,
    event      STRING,
    imei       STRING,
    imsi       STRING,
    info       STRING,
    type       INT
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

create table ssp_log_dwr(
    appId        INT,
    event        STRING,
    times        BIGINT,
    successTimes BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--mysql to hive 
CREATE TABLE log_stat(
  AppId             int,
  Event             varchar(100),
  Count             BIGINT,
  SuccessCount      BIGINT
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;
;



select * from (
   select
       rowkey,count(*) as sum
   from adx_ssp_dwi
   where b_date='2018-03-14' and repeated='N' group by rowkey
)t where t.sum >1 ;

select count(distinct event_key), count(event_key), b_date
from adx_ssp_dwi
where b_date ="2018-03-14" and repeated='N'
group by b_date
order by b_date;





------------------------------------------------------------------------------------
-- Hive FOR bigqurry
------------------------------------------------------------------------------------
drop view if exists ssp_log_dm;
create view ssp_log_dm as
select
    appId           as appid   ,
    a.name          as appname,
    a.publisherid   ,
    p.name          as publishername,
    event         ,
    times,
    successTimes    as successtimes,
    l_time        ,
    b_date,
    co.id           as companyid,
    co.name         as companyname
from ssp_log_dwr dwr
left join app a       on a.id = dwr.appid
left join publisher p on p.id = a.publisherId

left join employee e  on e.id = p.amId
left join proxy pro   on pro.id  = e.proxyId
left join company co  on co.id = pro.companyId
where b_date >= "2017-12-18"
union all
select
    appId           as appid,
    a.name          as appname,
    a.publisherid,
    p.name          as publishername,
    event,
    count           as times,
    successcount    as successtimes,
    '2000-01-01 00:00:00' as l_time,
    statdate        as b_date,
    co.id           as companyid,
    co.name         as companyname
from log_stat l
left join app a       on a.id = l.appid
left join publisher p on p.id = a.publisherId

left join employee e  on e.id = p.amId
left join proxy pro   on pro.id  = e.proxyId
left join company co  on co.id = pro.companyId
where statdate < "2017-12-18"
;

-- Greenplum
drop table if exists ssp_log_dm;
create table ssp_log_dm(
    appId               INT,
    appName             character varying(100),
    publisherId         INT,
    publisherName       character varying(100),
    event               VARCHAR(200),
    times               BIGINT,
    successTimes        BIGINT,
    l_time              VARCHAR(200),
    b_date              VARCHAR(200)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_log_dm_default_partition')
);


