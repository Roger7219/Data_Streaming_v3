------------------------------------------------
-- hive dm
------------------------------------------------
drop view sdk_dyn_dm;
create view sdk_dyn_dm as
select
  dwr.adv         as adv        ,
  u1.username     as adv_name   ,
  dwr.adv_bd      as adv_bd     ,
  u2.username     as adv_bd_name,
  dwr.jar         as jar        ,

  j.name          as jar_name   ,
  dwr.pub         as pub        ,
  u3.username     as pub_name   ,
  pub_bd          as pub_bd     ,
  u4.username     as pub_bd_name,

  dwr.app         as app        ,
  a.name          as app_name   ,
  dwr.country     as country    ,
  dwr.model       as model      ,
  dwr.brand       as brand      ,
  dwr.version     as version    ,

  dwr.requests                  ,
  dwr.responses                 ,

  dwr.loads                     ,
  dwr.success_loads             ,
  dwr.revenue                   ,
  dwr.cost                      ,

  dwr.active_users              ,
  dwr.new_users                 ,
  dwr.downloads                 ,
  dwr.success_downloads         ,


  dwr.l_time                    ,
  dwr.b_date                    ,
  dwr.b_time                    ,
  dwr.b_version
from sdk_dyn_dwr dwr
left join sys_user u1 on u1.user_id = dwr.adv
left join sys_user u2 on u2.user_id = dwr.adv_bd
left join jar j       on j.id       = dwr.jar
left join sys_user u3 on u3.user_id = dwr.pub
left join sys_user u4 on u4.user_id = dwr.pub_bd
left join app      a  on a.id       = dwr.app;


------------------------------------------------
-- clickhouse dm
------------------------------------------------
CREATE TABLE sdk_dyn_dm (
  adv           Int32 DEFAULT CAST(0 AS Int32)     ,
  adv_name      Nullable(String)                   ,
  adv_bd        Int32 DEFAULT CAST(0 AS Int32)     ,
  adv_bd_name   Nullable(String)                   ,
  jar           Int32 DEFAULT CAST(0 AS Int32)     ,

  jar_name      Nullable(String)                   ,
  pub           Int32 DEFAULT CAST(0 AS Int32)     ,
  pub_name      Nullable(String)                   ,
  pub_bd        Int32 DEFAULT CAST(0 AS Int32)     ,
  pub_bd_name   Nullable(String)                   ,

  app           Int32 DEFAULT CAST(0 AS Int32)     ,
  app_name      Nullable(String)                   ,
  country       Nullable(String)                   ,
  model         Nullable(String)                   ,
  brand         Nullable(String)                   ,
  version       Nullable(String)                   ,

  requests      Int64 DEFAULT CAST(0 AS Int64)     ,
  responses     Int64 DEFAULT CAST(0 AS Int64)     ,
  loads         Int64 DEFAULT CAST(0 AS Int64)     ,
  success_loads Int64 DEFAULT CAST(0 AS Int64)     ,
  revenue       Float64 DEFAULT CAST(0. AS Float64),
  cost          Float64 DEFAULT CAST(0. AS Float64),

  active_users      Int64 DEFAULT CAST(0 AS Int64) ,
  new_users         Int64 DEFAULT CAST(0 AS Int64) ,
  downloads         Int64 DEFAULT CAST(0 AS Int64) ,
  success_downloads Int64 DEFAULT CAST(0 AS Int64) ,


  l_time        DateTime                           ,
  b_date        Date                               ,
  b_time        DateTime                           ,
  b_version     Nullable(String)
)
ENGINE = MergeTree PARTITION BY (b_date, b_time) ORDER BY (b_date, b_time) SETTINGS index_granularity = 8192;

CREATE TABLE sdk_dyn_dm_all AS sdk_dyn_dm ENGINE = Distributed(bip_ck_cluster, default, sdk_dyn_dm, rand());

CREATE TABLE sdk_dyn_dm_for_select AS sdk_dyn_dm;
CREATE TABLE sdk_dyn_dm_for_select_all AS sdk_dyn_dm_for_select ENGINE = Distributed(bip_ck_cluster, default, sdk_dyn_dm_for_select, rand());

-- 2019-10-17新增字段
-- ALTER TABLE sdk_dyn_dm                add column downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_all            add column downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_for_select     add column downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_for_select_all add column downloads Int64 DEFAULT CAST(0 AS Int64);
--
-- ALTER TABLE sdk_dyn_dm                add column success_downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_all            add column success_downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_for_select     add column success_downloads Int64 DEFAULT CAST(0 AS Int64);
-- ALTER TABLE sdk_dyn_dm_for_select_all add column success_downloads Int64 DEFAULT CAST(0 AS Int64);

-- INSERT INTO sdk_dyn_dm_for_select_all
-- VALUES (1, 'adv_name_1', 2, 'adv_bd_name_2', 3
-- 	, 'jar_name_3', 4, 'pub_name_4', 5, 'pub_bd_name_5'
-- 	, 6, 'app_name_6', 'c1', 'm1', 'b1'
-- 	, 'v1', 1, 2, 3, 4
-- 	, 5, 6, 7, 8, '2019-09-09 09:09:09'
-- 	, '2019-09-09', '2019-09-09 09:09:09', '0');

-- http://localhost:5555/SdkDynOverallReport/?startDate=2019-08-01&endDate=2020-08-31&start=0&length=100&fields=adv,adv_bd,jar,pub,pub_bd,app,country,model,brand,version

