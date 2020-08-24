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