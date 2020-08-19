create view ssp_overall_events_dm as
select
    dwi.*,
    cam.name          as campaignname,
    ad.name           as advername,
    o.name            as offername,
    p.name            as publishername,
    a.name            as appname,
    c.name            as countryname,
    ca.name           as carriername,

    p.ampaId          as ampaid,--新角色id（渠道 AM id）区分之前DB的 am id
    p_amp.name        as ampaname,
    ad.amaaId         as amaaid,
    a_ama.name        as amaaname,
    cam.adverId       as adverid
from ssp_overall_events_dwi dwi
left join campaign cam      on cam.id = dwi.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join offer o           on o.id = dwi.offerId
left join publisher p       on p.id = dwi.publisherId
left join app a             on a.id = dwi.subId
left join country c         on c.id = dwi.countryId
left join carrier ca        on ca.id = dwi.carrierId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId;