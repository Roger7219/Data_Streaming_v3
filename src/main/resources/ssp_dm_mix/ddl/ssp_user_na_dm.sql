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
