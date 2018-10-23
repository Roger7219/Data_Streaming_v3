CREATE TABLE agg_traffic_dwr(
    jarId           INT,
    appId           INT,
    countryId       INT,
    carrierId       INT,
    connectType     INT,
    publisherId     INT,
    affSub          STRING,   --子渠道

    userNewCount    BIGINT,
    userActiveCount BIGINT,
    fillCount       BIGINT,
    sendCount       BIGINT,
    showCount       BIGINT,

    clickCount      BIGINT,
    cost            DOUBLE
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


-- Greenplum
create table agg_traffic_dm(
    jarId               INT,
    appId               INT,
    countryId           INT,
    carrierId           INT,
    connectType         INT,
    publisherId         INT,
    affSub              VARCHAR(200),   --子渠道
    userNewCount        BIGINT,
    userActiveCount     BIGINT,
    fillCount           BIGINT,
    sendCount           BIGINT,
    showCount           BIGINT,
    clickCount          BIGINT,
    cost                DECIMAL(19,10),
    l_time              VARCHAR(200),
    b_date              VARCHAR(200),


    publishername       character varying(100),
    jarname             character varying(100),
    appname             character varying(100),
    countryname         character varying(100),
    carriername         character varying(100),
    connecttypename     character varying(100)

)
WITH
(
    APPENDONLY=true, ORIENTATION=column, COMPRESSTYPE=zlib, COMPRESSLEVEL=5, BLOCKSIZE=1048576
)
PARTITION BY LIST(b_date)
(
    PARTITION __DEFAUT_PARTITION__ VALUES('-')  WITH (tablename='agg_traffic_dm_default_partition')
);

-- Hive For Greenplum
drop view if exists agg_traffic_dm;
create view agg_traffic_dm as
select
    dwr.jarId as jarid           ,
    dwr.appId as appid          ,
    dwr.countryId as countryid       ,
    dwr.carrierId as carrierid      ,
    dwr.connectType as connecttype    ,
    dwr.publisherId as publisherid    ,
    "" as affsub          ,   --子渠道

    dwr.userNewCount as usernewcount    ,
    dwr.userActiveCount as useractivecount ,
    dwr.fillCount as fillcount      ,
    dwr.sendCount as sendcount      ,
    dwr.showCount as showcount      ,

    dwr.clickCount as clickcount     ,
    dwr.cost,
    dwr.l_time ,
    dwr.b_date,

    concat_ws('^', p.name,  cast(dwr.publisherId  as string) ) as publishername,
--    "" as publishername,
    concat_ws('^', jc.name, cast(dwr.jarId       as string) ) as jarname, -- 其实属于jar_customer name
--    "" as jarname,
    concat_ws('^', a.name,  cast(dwr.appId       as string) ) as appname,
--    "" as appname,
    concat_ws('^', c.name,  cast(dwr.countryId   as string) ) as countryname,
--    "" as countryname,
    concat_ws('^', ca.name, cast(dwr.carrierId   as string) ) as carriername,
--    "" as carriername,
    concat_ws('^', ct.name, cast(dwr.connectType as string) ) as connecttypename
--    "" as connecttypename
from agg_traffic_dwr dwr
left join publisher p on p.ID = dwr.publisherId
left join jar_customer jc on jc.ID = dwr.jarId --  dwr.jarId 是JAR_CUSTOMER的id
left join app a on a.id = dwr.appId
left join country c on c.id = dwr.countryId
left join carrier ca on ca.id = dwr.carrierId
left join connect_type ct on ct.id = dwr.connectType;

-- FOR Kylin
drop view if exists agg_traffic_dm;
create view agg_traffic_dm as
select
    dwr.*,
    concat_ws('^', p.name,  cast(dwr.publisherId  as string) ) as publisherName,
    concat_ws('^', jc.name, cast(dwr.jarId       as string) ) as jarName, -- 其实属于jar_customer name
    concat_ws('^', a.name,  cast(dwr.appId       as string) ) as appName,
    concat_ws('^', c.name,  cast(dwr.countryId   as string) ) as countryName,
    concat_ws('^', ca.name, cast(dwr.carrierId   as string) ) as carrierName,
    concat_ws('^', ct.name, cast(dwr.connectType as string) ) as connectTypeName
from agg_traffic_dwr dwr
left join publisher p on p.ID = dwr.publisherId
left join jar_customer jc on jc.ID = dwr.jarId --  dwr.jarId 是JAR_CUSTOMER的id
left join app a on a.id = dwr.appId
left join country c on c.id = dwr.countryId
left join carrier ca on ca.id = dwr.carrierId
left join connect_type ct on ct.id = dwr.connectType;

create table connect_type(
    id   INT,
    name string
)
STORED AS ORC;

insert overwrite table connect_type
values(
1, "wifi"
) ,(
2, "gprs"
);



---------------------------------------
insert overwrite table agg_traffic_dwr partition(l_time, b_date)
select
    jarId,
    appId,
    countryId,
    carrierId,
    connectType,
    publisherId,
    affSub,

    sum(userNewCount) as userNewCount,
    sum(userActiveCount) as userActiveCount,
    sum(fillCount) as fillCount,
    sum(sendCount) as sendCount,
    sum(showCount) as showCount,
    sum(clickCount)  as clickCount,
    sum(cost) as cost,
    '2017-09-08 00:00:00' as l_time,
    b_date

from(

    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 as userNewCount,
        0 as userActiveCount,
        0 as fillCount,
        0 as sendCount,
        0 as showCount,
        count(1)  as clickCount,
        0 as cost,
        b_date
    from agg_click_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 as userNewCount,
        0 as userActiveCount,
        0 as fillCount,
        0 as sendCount,
        0 as showCount,
        0 as clickCount,
        sum(reportPrice) as cost,
        b_date
    from agg_fee_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 as userNewCount,
        0 as userActiveCount,
        count(1) as fillCount,
        0 as sendCount,
        0 as showCount,
        0  as clickCount,
        0 as cost,
        b_date
    from agg_fill_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 as userNewCount,
        0 as userActiveCount,
        0 as fillCount,
        count(1) as sendCount,
        0 as showCount,
        0  as clickCount,
        0 as cost,
        b_date
    from agg_send_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 as userNewCount,
        0 as userActiveCount,
        0 as fillCount,
        0 as sendCount,
        count(1) as showCount,
        0  as clickCount,
        0 as cost,
        b_date
    from agg_show_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        count( if(repeated = 'N', repeated, null) ) as userNewCount,
        0 as userActiveCount,
        0 as fillCount,
        0 as sendCount,
        0 as showCount,
        0 as clickCount,
        0 as cost,
        b_date
    from agg_user_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )
    union all (
    select
        jarId,
        appId,
        countryId,
        carrierId,
        connectType,
        publisherId,
        affSub,
        0 userNewCount,
        count( if(repeats >= 0, repeats, null) ) as userActiveCount,
        0 as fillCount,
        0 as sendCount,
        0 as showCount,
        0 as clickCount,
        0 as cost,
        b_date
    from agg_user_dwi
    group by
       jarId,
       appId,
       countryId,
       carrierId,
       connectType,
       publisherId,
       affSub,
       b_date
    )

) t5
group by
jarId,
appId,
countryId,
carrierId,
connectType,
publisherId,
affSub,
b_date
