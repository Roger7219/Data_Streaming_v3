create view ssp_log_dm as
select
    appId           as appid   ,
    a.name          as appname,
    a.publisherid   ,
    p.name          as publishername,
    event         ,
    times,
    successTimes    as successtimes,
    l_time        ,
    b_date,
    co.id           as companyid,
    co.name         as companyname
from ssp_log_dwr dwr
left join app a       on a.id = dwr.appid
left join publisher p on p.id = a.publisherId

left join employee e  on e.id = p.amId
left join proxy pro   on pro.id  = e.proxyId
left join company co  on co.id = pro.companyId
where b_date >= "2017-12-18"
union all
select
    appId           as appid,
    a.name          as appname,
    a.publisherid,
    p.name          as publishername,
    event,
    count           as times,
    successcount    as successtimes,
    '2000-01-01 00:00:00' as l_time,
    statdate        as b_date,
    co.id           as companyid,
    co.name         as companyname
from log_stat l
left join app a       on a.id = l.appid
left join publisher p on p.id = a.publisherId

left join employee e  on e.id = p.amId
left join proxy pro   on pro.id  = e.proxyId
left join company co  on co.id = pro.companyId
where statdate < "2017-12-18";