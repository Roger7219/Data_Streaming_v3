CREATE TABLE sdk_dyn_dwr(
  adv           int         ,
  adv_db        int         ,
  jar           string      ,
  pub           int         ,
  pub_bd        int         ,

  app           int         ,
  country       string      ,
  model         string      ,
  brand         string      ,
  version       string      ,

  requests      bigint      ,
  responses     bigint      ,
  loads         bigint      ,
  success_loads bigint      ,
  revenue       double      ,
  cost          double      ,

  active_users  bigint      ,
  new_users     bigint
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;