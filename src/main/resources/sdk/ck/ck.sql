------------------------------------------------
-- hive dm
------------------------------------------------
create view sdk_dyn_dm as
select * from sdk_dyn_dwr


------------------------------------------------
-- clickhouse dm
------------------------------------------------
CREATE TABLE sdk_dyn_dm (
  adv           Int32 DEFAULT CAST(0 AS Int32)     ,
  adv_name      Nullable(String)                   ,
  adv_db        Int32 DEFAULT CAST(0 AS Int32)     ,
  adv_db_name   Nullable(String)                   ,
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

  active_users  Int64 DEFAULT CAST(0 AS Int64)     ,
  new_users     Int64 DEFAULT CAST(0 AS Int64)     ,

  l_time        DateTime                           ,
  b_date        Date                               ,
  b_time        DateTime                           ,
  b_version     Nullable(String)
)
ENGINE = MergeTree PARTITION BY (b_date, b_time) ORDER BY (b_date, b_time) SETTINGS index_granularity = 8192;

CREATE TABLE sdk_dyn_dm_all AS sdk_dyn_dm ENGINE = Distributed(bip_ck_cluster, default, sdk_dyn_dm, rand());

CREATE TABLE sdk_dyn_dm_for_select AS sdk_dyn_dm;
CREATE TABLE sdk_dyn_dm_for_select_all AS sdk_dyn_dm_for_select ENGINE = Distributed(bip_ck_cluster, default, sdk_dyn_dm_for_select, rand());



-- INSERT INTO sdk_dyn_dm_for_select_all
-- VALUES (1, 'adv_name_1', 2, 'adv_db_name_2', 3
-- 	, 'jar_name_3', 4, 'pub_name_4', 5, 'pub_bd_name_5'
-- 	, 6, 'app_name_6', 'c1', 'm1', 'b1'
-- 	, 'v1', 1, 2, 3, 4
-- 	, 5, 6, 7, 8, '2019-09-09 09:09:09'
-- 	, '2019-09-09', '2019-09-09 09:09:09', '0');

-- http://localhost:5555/SdkDynOverallReport/?startDate=2019-08-01&endDate=2020-08-31&start=0&length=100&fields=adv,adv_db,jar,pub,pub_bd,app,country,model,brand,version

