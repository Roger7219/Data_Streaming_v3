CREATE TABLE sdk_dyn_dwr(
  adv               int   ,
  adv_bd            int   ,
  jar               string,
  pub               int   ,
  pub_bd            int   ,

  app               int   ,
  country           string,
  model             string,
  brand             string,
  version           string,

  requests          bigint,
  responses         bigint,
  loads             bigint,
  success_loads     bigint,
  revenue           double,
  cost              double,

  active_users      bigint,
  new_users         bigint,

  downloads         bigint,
  success_downloads bigint
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;

--
-- ALTER TABLE sdk_dyn_dwr ADD COLUMNS (downloads bigint);
-- ALTER TABLE sdk_dyn_dwr ADD COLUMNS (success_downloads bigint);