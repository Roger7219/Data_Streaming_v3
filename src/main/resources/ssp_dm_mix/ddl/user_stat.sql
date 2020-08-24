create table user_stat(
  AppId         int,
  CountryId     int,
  SdkVersion    varchar(20),
  UserCount     bigint,
  ActiveCount   bigint
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;