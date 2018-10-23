-------------------------------------------------------------
-- 新增用户数据
-------------------------------------------------------------
create table ssp_user_new_dwi(
    repeats      INT   ,
    rowkey       STRING,
    imei         STRING,
    imsi         STRING,
    createTime   STRING,
    activeTime   STRING,
    appId        INT,  -- publisherId, AM(EMPLOYEE)
    model        STRING,
    version      STRING,
    sdkVersion   INT,
    installType  INT,
    leftSize     STRING,
    androidId    STRING,
    userAgent    STRING,
    ipAddr       STRING,
    screen       STRING,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

ALTER TABLE ssp_user_new_dwi ADD COLUMNS (affSub STRING);
ALTER TABLE ssp_user_active_dwi ADD COLUMNS (affSub STRING);
ALTER TABLE ssp_user_keep_dwi ADD COLUMNS (affSub STRING);


create table ssp_user_new_dwr(
    appId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING,
    newCount     BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/22
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_user_new_dwr ADD COLUMNS (b_time STRING);
-------------------------------------------------------------
-- 活跃用户数据
-------------------------------------------------------------
create table ssp_user_active_dwi(
    repeats      INT,
    rowkey       STRING,
    imei         STRING,
    imsi         STRING,
    createTime   STRING,
    activeTime   STRING,
    appId        INT,  -- publisherId, AM
    model        STRING,
    version      STRING,
    sdkVersion   INT,
    installType  INT,
    leftSize     STRING,
    androidId    STRING,
    userAgent    STRING,
    ipAddr       STRING,
    screen       STRING,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING,
    lat          string,
    lon          string,
    mac1         string,
    mac2         string,
    ssid         string,
    lac          int,
    cellid       int,
    ctype        int
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;


create table ssp_user_active_dwr(
    appId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING,
    activeCount  BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/22
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_user_active_dwr ADD COLUMNS (b_time STRING);


-------------------------------------------------------------
-- 留存用户数据
-------------------------------------------------------------
create table ssp_user_keep_dwi(
    repeats      INT,
    rowkey       STRING,
    imei         STRING,
    imsi         STRING,
    createTime   STRING,
    activeTime   STRING,
    appId        INT,  -- publisherId, AM
    model        STRING,
    version      STRING,
    sdkVersion   INT,
    installType  INT,
    leftSize     STRING,
    androidId    STRING,
    userAgent    STRING,
    ipAddr       STRING,
    screen       STRING,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING
)
PARTITIONED BY (repeated STRING, l_time STRING, b_date STRING)
STORED AS ORC;

create table ssp_user_keep_dwr(
    appId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    affSub       STRING,
    activeDate   STRING,
    userCount    BIGINT,
    firstCount   BIGINT,
    secondCount  BIGINT,
    thirdCount   BIGINT,
    fourthCount  BIGINT,
    fifthCount   BIGINT,
    sixthCount   BIGINT,
    seventhCount BIGINT,
    fiftyCount   BIGINT,
    thirtyCount  BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--add new clomns 12/22
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (operatingSystem STRING);
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (systemLanguage STRING);
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (deviceBrand STRING);
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (deviceType STRING);
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (browserKernel STRING);
ALTER TABLE ssp_user_keep_dwr ADD COLUMNS (b_time STRING);

-- From MYSQL for User Reports
drop table if exists user_stat;
create table user_stat(
  AppId         int,
  CountryId     int,
  SdkVersion    varchar(20),
  UserCount     bigint,
  ActiveCount   bigint
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;
;

drop table if exists user_retention;
CREATE TABLE user_retention(
  AppId             int,
  CountryId         int,
  SdkVersion        varchar(20),
  UserCount         bigint,
  FirstCount        bigint,
  SecondCount       bigint,
  ThirdCount        bigint,
  FourthCount       bigint,
  FifthCount        bigint,
  SixthCount        bigint,
  SeventhCount      bigint,
  FiftyCount        bigint,
  ThirtyCount       bigint
)
PARTITIONED BY (StatDate STRING)
STORED AS ORC;
;

-- [{"activeTime":"2017-08-21 16:58:06.0","affSub":"","androidId":"df62fbcee73182c3","appId":1859,"carrierId":600,"countryId":215,"createTime":"2017-09-07 16:23:05","imei":"359286077613794","imsi":"434054401651087","installType":0,"ipAddr":"188.113.210.30","leftSize":"119520","model":"BLUSTUDIOX8HD","screen":"720x1280","sdkVersion":0,"sv":"v1.4.0","userAgent":"Mozilla/5.0 (Linux; Android 4.4.2; BLU STUDIO X8 HD Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36","version":"4.4.2"}

------------------------------------------------------------------------------------------------------------------------
--Hive For Greenplum
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_user_na_dm;
create view ssp_user_na_dm as
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    un.countryId    as countryid,
    carrierId       as carrierid,
    v.id            as versionid,
    newCount        as newcount,--  BIGINT,
    0               as activecount, -- BIGINT
    l_time,
    b_date,
    un.sv           as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    ca.name         as carriername,
    co.id           as companyid,
    co.name         as companyname,
    p.ampaId        as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name      as publisherampaname
from ssp_user_new_dwr un
left join app a             on a.id      = un.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = un.sv
left join country c         on c.id      = un.countryId
left join carrier ca        on ca.id     = un.carrierId

--left join publisher a_p     on  a_p.id = a.publisherId
--left join employee ap_am    on  ap_am.id  = a_p.amId
left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId

left join employee p_amp    on p_amp.id = p.ampaId
where v.id is not null and b_date >= "2017-12-18"
union all
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    ua.countryId       as countryid,
    carrierId       as carrierid,
    v.id            as versionid,
    0               as newcount,
    activeCount     as activecount,
    l_time,
    b_date,
    ua.sv           as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    ca.name         as carriername,
    co.id           as companyid,
    co.name         as companyname,
    p.ampaId        as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name      as publisherampaname
from ssp_user_active_dwr ua
left join app a             on a.id      = ua.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = ua.sv
left join country c         on c.id      = ua.countryId
left join carrier ca        on ca.id     = ua.carrierId

left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId
left join employee p_amp    on p_amp.id = p.ampaId
where v.id is not null and b_date >= "2017-12-18"
union all
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    u.countryId     as countryid,
    -1              as carrierid,
    v.id            as versionid,
    usercount       as newcount,
    activeCount     as activecount,
    '2000-01-01 00:00:00' as l_time,
    statDate        as b_date,
    u.sdkversion    as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    null            as carriername,
    co.id           as companyid,
    co.name         as companyname,
    p.ampaId        as publisherampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name      as publisherampaname
from user_stat u
left join app a             on a.id      = u.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = u.sdkversion
left join country c         on c.id      = u.countryId
--left join carrier ca        on ca.id     = u.carrierId

left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId
left join employee p_amp    on p_amp.id = p.ampaId
where v.id is not null and statDate < "2017-12-18" ;


drop view if exists ssp_user_keep_dm;
create view ssp_user_keep_dm as
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    uk.countryId    as countryid,
    carrierId       as carrierid,
    v.id            as versionid,
    0               as newcount,
    firstCount      as firstcount,
    secondCount     as secondcount,
    thirdCount      as thirdcount,
    fourthCount     as fourthcount,
    fifthCount      as fifthcount,
    sixthCount      as sixthcount,
    seventhCount    as seventhcount,
    fiftyCount      as fiftycount,
    thirtyCount     as thirtycount,
    l_time,
    b_date          as b_date,
    activeDate      as statDate,
    uk.sv           as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    ca.name         as carriername,
    co.id           as companyid,
    co.name         as companyname
from ssp_user_keep_dwr uk
left join app a             on a.id      = uk.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = uk.sv
left join country c         on c.id      = uk.countryId
left join carrier ca        on ca.id     = uk.carrierId

left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId
where v.id is not null and activeDate >= "2017-12-18"
union all
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    un.countryId    as countryid,
    carrierId       as carrierid,
    v.id            as versionid,
    newCount        as newcount,
    0               as firstcount ,--  BIGINT,
    0               as secondcount,--  BIGINT,
    0               as thirdcount ,--  BIGINT,
    0               as fourthcount,--  BIGINT,
    0               as fifthcount ,--  BIGINT,
    0               as sixthcount ,--  BIGINT,
    0               as seventhcount,-- BIGINT,
    0               as fiftycount  ,-- BIGINT,
    0               as thirtycount, -- BIGINT
    l_time,
    b_date,
    b_date          as statdate,
    un.sv           as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    ca.name         as carriername,
    co.id           as companyid,
    co.name         as companyname
from ssp_user_new_dwr un
left join app a             on a.id      = un.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = un.sv
left join country c         on c.id      = un.countryId
left join carrier ca        on ca.id     = un.carrierId

left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId
where v.id is not null and b_date >= "2017-12-18"
union all
select
    a.publisherId   as publisherid,
    p.amId          as amid,
    appId           as appid,
    u.countryId     as countryid,
    -1              as carrierid,
    v.id            as versionid,
    usercount       as newcount,
    firstCount      as firstcount,
    secondCount     as secondcount,
    thirdCount      as thirdcount,
    fourthCount     as fourthcount,
    fifthCount      as fifthcount,
    sixthCount      as sixthcount,
    seventhCount    as seventhcount,
    fiftyCount      as fiftycount,
    thirtyCount     as thirtycount,
    '2000-01-01 00:00:00' as l_time,
    statdate        as b_date,
    statdate        as statdate,
    u.sdkversion    as versionname,
    p.name          as publishername,
    e.name          as amname,
    a.name          as appname,
    c.name          as countryname,
    null            as carriername,
    co.id           as companyid,
    co.name         as companyname
from user_retention u
left join app a             on a.id      = u.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = u.sdkversion
left join country c         on c.id      = u.countryId
--left join carrier ca        on ca.id     = u.carrierId

left join proxy pro         on  pro.id  = e.proxyId
left join company co        on  co.id = pro.companyId
where v.id is not null and statdate < "2017-12-18" ;

------------------------------------------------------------------------------------------------------------------------
-- For Greenplum
------------------------------------------------------------------------------------------------------------------------
drop table if exists ssp_user_na_dm;
create table ssp_user_na_dm(
    publisherid     int,
    amid            int,
    appid           int,
    countryid       int,
    carrierid       int,
    versionid       int,
    newcount        bigint,
    activecount     bigint,
    l_time          VARCHAR(100),
    b_date          VARCHAR(100),
    versionname     VARCHAR(100),
    publishername   VARCHAR(100),
    amname          VARCHAR(100),
    appname         VARCHAR(100),
    countryname     VARCHAR(100),
    carriername     VARCHAR(100)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_user_na_dm_default_partition')
);


drop table if exists ssp_user_keep_dm;
create table ssp_user_keep_dm(
    publisherid     int,
    amid            int,
    appid           int,
    countryid       int,
    carrierid       int,
    versionid       int,
    newcount        bigint,
    firstcount      bigint,
    secondcount     bigint,
    thirdcount      bigint,
    fourthcount     bigint,
    fifthcount      bigint,
    sixthcount      bigint,
    seventhcount    bigint,
    fiftycount      bigint,
    thirtycount     bigint,
    l_time          VARCHAR(100),
    b_date          VARCHAR(100),
    versionname     VARCHAR(100),
    publishername   VARCHAR(100),
    amname          VARCHAR(100),
    appname         VARCHAR(100),
    countryname     VARCHAR(100),
    carriername     VARCHAR(100)
)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='ssp_user_keep_dm_default_partition')
);



------------------------------------------------------------------------------------------------------------------------
-- For Kylin
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_user_na_dm;
create view ssp_user_na_dm as
select
    a.publisherId,
    p.amId,
    appId      ,--  INT,
    un.countryId  ,--  INT,
    carrierId  ,--  INT,
    v.id   as versionId,
    newCount   ,--  BIGINT,
    null as activeCount, -- BIGINT
    l_time,
    b_date,
    concat_ws('^', un.sv ,   cast(v.id as string) )              as versionName,
    concat_ws('^', p.name ,  cast(a.publisherId as string) )     as publisherName,
    concat_ws('^', e.name ,  cast(p.amId as string) )            as amName,
    concat_ws('^', a.name ,  cast(un.appId as string) )          as appName,
    concat_ws('^', c.name,  cast(un.countryId as string) ) as countryName,
    concat_ws('^', ca.name , cast(un.carrierId as string) )      as carrierName
from ssp_user_new_dwr un
left join app a             on a.id      = un.appId
left join publisher p       on p.id      =  a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = un.sv
left join country c         on c.id      = un.countryId
left join carrier ca        on ca.id     = un.carrierId
union all
select
    a.publisherId,
    p.amId,
    appId      ,--  INT,
    ua.countryId  ,--  INT,
    carrierId  ,--  INT,
--    sv         ,--  STRING,
    v.id as versionId,
    0 as newCount   ,--  BIGINT,
    activeCount, -- BIGINT
    l_time,
    b_date,
    concat_ws('^', ua.sv ,   cast(v.id as string) )              as versionName,
    concat_ws('^', p.name ,  cast(a.publisherId as string) )     as publisherName,
    concat_ws('^', e.name ,  cast(p.amId as string) )            as amName,
    concat_ws('^', a.name ,  cast(ua.appId as string) )          as appName,
    concat_ws('^', c.name,  cast(ua.countryId as string) ) as countryName,
    concat_ws('^', ca.name , cast(ua.carrierId as string) )      as carrierName
from ssp_user_active_dwr ua
left join app a             on a.id      = ua.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = ua.sv
left join country c         on c.id      = ua.countryId
left join carrier ca        on ca.id     = ua.carrierId;


drop view if exists ssp_user_keep_dm;
create view ssp_user_keep_dm as
select
    a.publisherId,
    p.amId,
    appId      ,--  INT,
    uk.countryId  ,--  INT,
    carrierId  ,--  INT,
--    sv         ,--  STRING,
    v.id as versionId,
--    activeDate ,--  STRING,
--    userCount  ,--  BIGINT,
    null as newCount,
    firstCount ,--  BIGINT,
    secondCount,--  BIGINT,
    thirdCount ,--  BIGINT,
    fourthCount,--  BIGINT,
    fifthCount ,--  BIGINT,
    sixthCount ,--  BIGINT,
    seventhCount,-- BIGINT,
    fiftyCount  ,-- BIGINT,
    thirtyCount, -- BIGINT
    l_time,
    b_date,
    activeDate as statDate,
    concat_ws('^', uk.sv ,   cast(v.id as string) )              as versionName,
    concat_ws('^', p.name ,  cast(a.publisherId as string) )     as publisherName,
    concat_ws('^', e.name ,  cast(p.amId as string) )            as amName,
    concat_ws('^', a.name ,  cast(uk.appId as string) )          as appName,
    concat_ws('^', c.name,  cast(uk.countryId as string) )       as countryName,
    concat_ws('^', ca.name , cast(uk.carrierId as string) )      as carrierName
from ssp_user_keep_dwr uk
left join app a             on a.id      = uk.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = uk.sv
left join country c         on c.id      = uk.countryId
left join carrier ca        on ca.id     = uk.carrierId
union all
select
    a.publisherId,
    p.amId,
    appId      ,--  INT,
    un.countryId  ,--  INT,
    carrierId  ,--  INT,
--    sv         ,--  STRING,
    v.id as versionId,
    newCount   ,--  BIGINT,
    0 as firstCount ,--  BIGINT,
    0 as secondCount,--  BIGINT,
    0 as thirdCount ,--  BIGINT,
    0 as fourthCount,--  BIGINT,
    0 as fifthCount ,--  BIGINT,
    0 as sixthCount ,--  BIGINT,
    0 as seventhCount,-- BIGINT,
    0 as fiftyCount  ,-- BIGINT,
    0 as thirtyCount, -- BIGINT
    l_time,
    b_date,
    b_date as statDate,
    concat_ws('^', un.sv ,   cast(v.id as string) )              as versionName,
    concat_ws('^', p.name ,  cast(a.publisherId as string) )     as publisherName,
    concat_ws('^', e.name ,  cast(p.amId as string) )            as amName,
    concat_ws('^', a.name ,  cast(un.appId as string) )          as appName,
    concat_ws('^', c.name,  cast(un.countryId as string) )       as countryName,
    concat_ws('^', ca.name , cast(un.carrierId as string) )      as carrierName
from ssp_user_new_dwr un
left join app a             on a.id      = un.appId
left join publisher p       on p.id      = a.publisherId
left join employee e        on e.id      = p.amId
left join version_control v on v.version = un.sv
left join country c         on c.id      = un.countryId
left join carrier ca        on ca.id     = un.carrierId;


 CREATE VIEW ssp_user_keep_dm AS select                                        
    a.publisherid   as publisherid,                                       
    p.amid          as amid,                                              
    uk.appid           as appid,                                          
    uk.countryid    as countryid,                                         
    uk.carrierid       as carrierid,                                      
    v.id            as versionid,                                         
    0               as newcount,                                              
    uk.firstcount      as firstcount,                                     
    uk.secondcount     as secondcount,                                    
    uk.thirdcount      as thirdcount,                                     
    uk.fourthcount     as fourthcount,                                    
    uk.fifthcount      as fifthcount,                                     
    uk.sixthcount      as sixthcount,                                     
    uk.seventhcount    as seventhcount,                                   
    uk.fiftycount      as fiftycount,                                     
    uk.thirtycount     as thirtycount,                                    
    uk.l_time,
    uk.b_date,
    uk.activedate      as statdate,
    uk.sv           as versionname,                                       
    p.name          as publishername,                                     
    e.name          as amname,                                            
    a.name          as appname,                                           
    c.name          as countryname,                                       
    ca.name         as carriername                                        
from default.ssp_user_keep_dwr uk                                         
left join default.app a             on a.id      = uk.appid       
left join default.publisher p       on p.id      = a.publisherid  
left join default.employee e        on e.id      = p.amid         
left join default.version_control v on v.version = uk.sv          
left join default.country c         on c.id      = uk.countryid   
left join default.carrier ca        on ca.id     = uk.carrierid   
union all                                                                       
select                                                                          
    a.publisherid   as publisherid,                                       
    p.amid          as amid,                                              
    un.appid           as appid,                                          
    un.countryid    as countryid,                                         
    un.carrierid       as carrierid,                                      
    v.id            as versionid,                                         
    un.newcount        as newcount,                                       
    0               as firstcount ,--  BIGINT,                                
    0               as secondcount,--  BIGINT,                                
    0               as thirdcount ,--  BIGINT,                                
    0               as fourthcount,--  BIGINT,                                
    0               as fifthcount ,--  BIGINT,                                
    0               as sixthcount ,--  BIGINT,                                
    0               as seventhcount,-- BIGINT,                                
    0               as fiftycount  ,-- BIGINT,                                
    0               as thirtycount, -- BIGINT                                 
    un.l_time,                                                              
    un.b_date,
    un.b_date       as statdate,
    un.sv           as versionname,
    p.name          as publishername,                                     
    e.name          as amname,                                            
    a.name          as appname,                                           
    c.name          as countryname,                                       
    ca.name         as carriername                                        
from default.ssp_user_new_dwr un                                          
left join default.app a             on a.id      = un.appid       
left join default.publisher p       on p.id      = a.publisherid  
left join default.employee e        on e.id      = p.amid         
left join default.version_control v on v.version = un.sv          
left join default.country c         on c.id      = un.countryid   
left join default.carrier ca        on ca.id     = un.carrierid


CREATE TABLE ssp_user_keep_dm (
    publisherid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    amid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    appid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    countryid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    carrierid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    versionid integer ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    newcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    firstcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    secondcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    thirdcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    fourthcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    fifthcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    sixthcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    seventhcount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    fiftycount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    thirtycount bigint ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    l_time character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    b_date character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    statdate character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    versionname character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    publishername character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    amname character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    appname character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    countryname character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576),
    carriername character varying(100) ENCODING (compresstype=zlib,compresslevel=5,blocksize=1048576)
)
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5, blocksize=1048576) DISTRIBUTED BY (publisherid) PARTITION BY LIST(b_date)
          (
          PARTITION __defaut_partition__ VALUES('-') WITH (tablename='ssp_user_keep_dm_default_partition', appendonly=true, orientation=column, compresstype=zlib, compresslevel=5, blocksize=1048576 )
                    COLUMN publisherid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN amid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN appid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN countryid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN carrierid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN versionid ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN newcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN firstcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN secondcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN thirdcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN fourthcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN fifthcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN sixthcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN seventhcount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN fiftycount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN thirtycount ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN l_time ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN b_date ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN versionname ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN publishername ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN amname ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN appname ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN countryname ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576)
                    COLUMN carriername ENCODING (compresstype=zlib, compresslevel=5, blocksize=1048576),
          PARTITION "2017_09_15" VALUES('2017-09-15') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_15', appendonly=false ),
          PARTITION "2017_09_16" VALUES('2017-09-16') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_16', appendonly=false ),
          PARTITION "2017_09_17" VALUES('2017-09-17') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_17', appendonly=false ),
          PARTITION "2017_09_18" VALUES('2017-09-18') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_18', appendonly=false ),
          PARTITION "2017_09_19" VALUES('2017-09-19') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_19', appendonly=false ),
          PARTITION "2017_09_20" VALUES('2017-09-20') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_20', appendonly=false ),
          PARTITION "2017_09_21" VALUES('2017-09-21') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_21', appendonly=false ),
          PARTITION "2017_09_22" VALUES('2017-09-22') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_22', appendonly=false ),
          PARTITION "2017_09_23" VALUES('2017-09-23') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_23', appendonly=false ),
          PARTITION "2017_09_24" VALUES('2017-09-24') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_24', appendonly=false ),
          PARTITION "2017_09_25" VALUES('2017-09-25') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_25', appendonly=false ),
          PARTITION "2017_09_26" VALUES('2017-09-26') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_26', appendonly=false ),
          PARTITION "2017_09_27" VALUES('2017-09-27') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_27', appendonly=false ),
          PARTITION "2017_09_28" VALUES('2017-09-28') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_28', appendonly=false ),
          PARTITION "2017_09_29" VALUES('2017-09-29') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_29', appendonly=false ),
          PARTITION "2017_09_30" VALUES('2017-09-30') WITH (tablename='ssp_user_keep_dm_1_prt_2017_09_30', appendonly=false ),
          PARTITION "2017_10_01" VALUES('2017-12-18') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_01', appendonly=false ),
          PARTITION "2017_10_02" VALUES('2017-10-02') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_02', appendonly=false ),
          PARTITION "2017_10_03" VALUES('2017-10-03') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_03', appendonly=false ),
          PARTITION "2017_10_04" VALUES('2017-10-04') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_04', appendonly=false ),
          PARTITION "2017_10_05" VALUES('2017-10-05') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_05', appendonly=false ),
          PARTITION "2017_10_06" VALUES('2017-10-06') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_06', appendonly=false ),
          PARTITION "2017_10_07" VALUES('2017-10-07') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_07', appendonly=false ),
          PARTITION "2017_10_08" VALUES('2017-10-08') WITH (tablename='ssp_user_keep_dm_1_prt_2017_10_08', appendonly=false )
          );



------------------------------------------------------------------------------------------------------------------------
-- 离线统计 alter table
------------------------------------------------------------------------------------------------------------------------
drop view if exists ssp_user_new_dwr_view;
create view ssp_user_new_dwr_view as
select
    appId    ,
    countryId,
    carrierId,
    sv       ,
    affSub   ,
    count(1)    as newCount,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
     b_date
from ssp_user_new_dwi
group by
   appId    ,
   countryId,
   carrierId,
   sv       ,
   affSub   ,
   date_format(l_time, 'yyyy-MM-dd 00:00:ss') ,
   b_date;


--create table ssp_user_new_dwr_tmp like ssp_user_new_dwr;

set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_user_new_dwr
select * from ssp_user_new_dwr_view
where second(l_time) = 0;
-- where l_time <='2017-09-09 00:00:00';


drop view if exists ssp_user_active_dwr_view;
create view ssp_user_active_dwr_view as
select
    appId    ,
    countryId,
    carrierId,
    sv       ,
    affSub   ,
    count(1)    as activeCount,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss')  as l_time,
     b_date
from ssp_user_active_dwi
group by
   appId    ,
   countryId,
   carrierId,
   sv       ,
   affSub   ,
   date_format(l_time, 'yyyy-MM-dd 00:00:ss') ,
   b_date;


--create table ssp_user_active_dwr_tmp like ssp_user_active_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_user_active_dwr
select * from ssp_user_active_dwr_view
where second(l_time) = 0;
-- where l_time <='2017-09-09 00:00:00';


--
--    select
--    version,
--    count(1) as activeCount
--    from ssp_user_active_dwi
--    where appid = 1960 and b_date = '2017-09-10'
--    group by version


drop view if exists ssp_user_keep_dwr_view;
create view ssp_user_keep_dwr_view as
select
    appId,
    countryId,
    carrierId,
    sv,
    affSub,
    split(activeTime, ' ')[0]  as activeDate,
    count(1)  as userCount,
    count( if(  (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24)  = 1, 1, null) ) as firstCount,
    count( if(  (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24)  = 2, 1, null) ) as secondCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 3, 1, null) ) as thirdCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 4, 1, null) ) as fourthCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 5, 1, null) ) as fifthCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 6, 1, null) ) as sixthCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 7, 1, null) )  as seventhCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 15, 1, null) )  as fiftyCount,
    count( if( (unix_timestamp(  concat(split(createTime, ' ')[0], ' 00:00:00')) - unix_timestamp(   concat(split(activeTime, ' ')[0], ' 00:00:00')) ) /(60*60*24) = 30, 1, null) ) as thirtyCount,
    date_format(l_time, 'yyyy-MM-dd 00:00:ss') as l_time,
    b_date
from ssp_user_active_dwi
group by
   appId    ,
   countryId,
   carrierId,
   sv       ,
   affSub   ,
   split(activeTime, ' ')[0],
   date_format(l_time, 'yyyy-MM-dd 00:00:ss'),
   b_date;


--create table ssp_user_keep_dwr_tmp like ssp_user_keep_dwr;
set spark.sql.shuffle.partitions=1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ssp_user_keep_dwr partition(l_time, b_date)
select * from ssp_user_keep_dwr_view
where second(l_time) = 0;
--where l_time <='2017-09-09 00:00:00';



























--------------------------------------------------------------------------------------------
-- TEST
create table dual3(g string,d double, bd decimal(19,10))

insert into table dual3 values("asd",3.31, 3.32)



select sum(reportPrice),b_date from ssp_fee_dwi group by b_date;










create view ssp_user_new_and_active_detail_dwr as
select
s.appid, a.name AS appName ,
a.PUBLISHERID ,p.name as publisherName,
p.amId, e.name as  amName,
s.countryId,
c.name as countryName,
s.carrierId,
ca.name as carrierName,
v.id as versionId,
v.version as versionName,
s.newCount   ,
s.activeCount,
s.fl_time    ,
s.b_date as statDate
from ssp_user_new_and_active_dwr s
left join APP a on a.id  = s.appid
left join PUBLISHER p on  p.id =  a.PUBLISHERID
left join EMPLOYEE e on  e.id =  p.amid

left join COUNTRY  c on c.id = s.countryId
left join carrier ca on ca.id =s.carrierId
left join VERSION_CONTROL v on v.version = s.sv and v.type = 2;


 publisherId,    --INT,
    p.amId,         -- amId 对应 employee.id
    subId as appId, --INT,
    c.countryId,    --INT,
    carrierId,    --INT,
    -- sv,         --STRING,
    v.id as versionId,      -- INT
    adType as adFormatId,   --INT, (adFormatId对应offer.adtype,逗号分隔，需展开)
    campaignId,     --INT,
    offerId,        --Int,
    ca.adCategory1, --INT 关联campaign.adCategory1
    ca.adVerid,

===

















create 'ssp_user_keep_dwi_uuid_stat', 'f'

-- Order: dwrGroupBy b_date aggExprsAlias l_time
CREATE TABLE SSP_USER_KEEP_DM_PHOENIX (
    APPID        INTEGER ,
    COUNTRYID    INTEGER ,
    CARRIERID    INTEGER ,
    SV           VARCHAR ,
    ACTIVEDATE   VARCHAR ,
    B_DATE       VARCHAR ,
    USERCOUNT    BIGINT,
    FIRSTCOUNT   BIGINT,
    SECONDCOUNT  BIGINT,
    THIRDCOUNT   BIGINT,
    FOURTHCOUNT  BIGINT,
    FIFTHCOUNT   BIGINT,
    SIXTHCOUNT   BIGINT,
    SEVENTHCOUNT BIGINT,
    FIFTYCOUNT   BIGINT,
    THIRTYCOUNT  BIGINT,
    L_TIME       VARCHAR,
    --    upsert into 字符串字段值要用单引号！！！
    constraint pk primary key (APPID,COUNTRYID,CARRIERID,SV,ACTIVEDATE,B_DATE)
);




------
create view ssp_user_keep_detail_dwr as
select
s.appid, a.name AS appName ,
a.PUBLISHERID ,p.name as publisherName,
p.amId, e.name as  amName,
s.countryId,
c.name as countryName,
s.carrierId,
ca.name as carrierName,
v.id as versionId,
v.version as versionName,
s.activeDate as statDate,
s.userCount    ,
s.firstCount   ,
s.secondCount  ,
s.thirdCount   ,
s.fourthCount  ,
s.fifthCount   ,
s.sixthCount   ,
s.seventhCount ,
s.fiftyCount   ,
s.thirtyCount  ,
s.fl_time      ,
s.b_date
from ssp_user_keep_dwr s
left join app a on a.id  = s.appid
left join PUBLISHER p on  p.id =  a.PUBLISHERID
left join EMPLOYEE e on  e.id =  p.amid

left join COUNTRY  c on c.id = s.countryId
left join carrier ca on ca.id =s.carrierId
left join VERSION_CONTROL v on v.version = s.sv and v.type = 2;




#v1
create view ssp_user_keep_dwr_kylin_v1 as
select
s.appid,
-- a.name AS appName ,
a.publisherid ,
-- p.name as publisherName,
p.amId,
--e.name as  amName,
s.countryId,
--c.name as countryName,
s.carrierId,
--ca.name as carrierName,
v.id as versionId,
--v.version as versionName,
s.activeDate as statDate,
s.userCount    ,
s.firstCount   ,
s.secondCount  ,
s.thirdCount   ,
s.fourthCount  ,
s.fifthCount   ,
s.sixthCount   ,
s.seventhCount ,
s.fiftyCount   ,
s.thirtyCount  ,
s.fl_time      ,
s.b_date
from ssp_user_keep_dwr s
left join app a on a.id  = s.appid
left join PUBLISHER p on  p.id =  a.PUBLISHERID
left join EMPLOYEE e on  e.id =  p.amid

left join COUNTRY  c on c.id = s.countryId
left join carrier ca on ca.id =s.carrierId
left join VERSION_CONTROL v on v.version = s.sv and v.type = 2;











select data
from dual
group by data
left









--test
create view ssp_user_keep_detail_dwr as
select
s.appid, a.name AS appName ,
a.PUBLISHERID ,p.name as publisherName,
p.amId, e.name as  amName,
s.countryId,
c.name as countryName,
s.carrierId,
ca.name as carrierName,
1 as versionId,
"1.0" as versionName,
s.activeDate as statDate,
s.userCount    ,
s.firstCount   ,
s.secondCount  ,
s.thirdCount   ,
s.fourthCount  ,
s.fifthCount   ,
s.sixthCount   ,
s.seventhCount ,
s.fiftyCount   ,
s.thirtyCount  ,
s.fl_time
from ssp_user_keep_dwr s
left join app a on a.id  = s.appid
left join PUBLISHER p on  p.id =  a.PUBLISHERID
left join EMPLOYEE e on  e.id =  p.amid

left join COUNTRY  c on c.id = s.countryId
left join carrier ca on ca.id =s.carrierId
--left join VERSION_CONTROL v on v.version = s.sv and v.type = 2

























select sum(activeCount),b_date from ssp_user_active_dwr where b_date >="2017-11-01"  group by b_date order by 1 desc;

select count(1),b_date from ssp_user_active_dwi where b_date >="2017-11-01" group by b_date  order by 1 desc;

--
select sum(activeCount),b_date from bk_ssp_user_active_dwr where b_date >="2017-11-01"  group by b_date order by 1 desc;

select count(1),b_date from bk_ssp_user_active_dwi where b_date >="2017-11-01" group by b_date  order by 1 desc;



insert overwrite table ssp_user_active_dwi select * from bk_ssp_user_active_dwi where b_date = "2017-11-01"



SELECT
statdate as statdate,
sum(newCount) as usercount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(firstCount)/sum(newCount) ELSE 0  END) AS INT64 )/10000.0  as firstcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(secondCount)/sum(newCount) ELSE 0 END) AS INT64 )/10000.0  as secondcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(thirdCount)/sum(newCount) ELSE 0  END) AS INT64 )/10000.0  as thirdcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(fourthCount)/sum(newCount) ELSE 0 END) AS INT64 )/10000.0  as fourthcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(fifthCount)/sum(newCount) ELSE 0  END) AS INT64 )/10000.0  as fifthcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(sixthCount)/sum(newCount) ELSE 0  END) AS INT64 )/10000.0  as sixthcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(seventhCount)/sum(newCount) ELSE 0 END) AS INT64)/10000.0 as seventhcount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(fiftyCount)/sum(newCount) ELSE 0  END) AS INT64 )/10000.0 as fiftycount,
CAST(10000.0* (CASE WHEN sum(newCount) > 0 THEN 1.0*sum(thirtyCount)/sum(newCount) ELSE 0 END) AS INT64 )/10000.0 as thirtycount
FROM report_dataset.ssp_user_keep_dm dm
WHERE
  dm.statDate <= '2017-11-04'
  AND dm.statDate >= '2017-12-18'
GROUP BY statDate
ORDER BY statDate ASC
LIMIT 10