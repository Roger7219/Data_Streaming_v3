
-- Hive
CREATE TABLE advertiser(
  id            int   ,
  user_id       int   ,
  advertiser_bd int   ,
  update_time   bigint
)
STORED AS ORC;

CREATE TABLE jar(
  id            int   ,
  advertiser_id int   ,
  name          string,
  create_time   bigint,
  update_time   bigint
)
STORED AS ORC;

CREATE TABLE publisher(
  id           int   ,
  publisher_bd int   ,
  user_id      int   ,
  update_time  bigint
)
STORED AS ORC;

CREATE TABLE app(
  id                  int   ,
  name                string,
  status              int   ,
  publisher_id        int   ,
  time_interval       int   ,
  load_times          int   ,
  country_filter      string,
  country_filter_type int   ,
  delete_all          int   ,
  create_time         bigint,
  update_time         bigint
)
STORED AS ORC;

CREATE TABLE sys_user(
  user_id     int,
  email       string,
  username    string,
  password    string,
  create_time bigint,
  update_time bigint,
  lock_flag   string,
  del_flag    string
)
STORED AS ORC;