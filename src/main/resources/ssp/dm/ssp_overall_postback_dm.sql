create view ssp_overall_postback_dm as
select
--    dwi.*,
    dwi.repeats,
    dwi.rowkey,
    dwi.id,
    dwi.publisherid,
    dwi.subid,
    dwi.offerid,
    dwi.campaignid,
    dwi.countryid,
    dwi.carrierid,
    dwi.devicetype,
    dwi.useragent,
    dwi.ipaddr,
    dwi.clickid,
    dwi.price,
    dwi.reporttime,
    dwi.createtime,
    dwi.clicktime,
    dwi.showtime,
    dwi.requesttype,
    dwi.pricemethod,
    dwi.bidprice,
    dwi.adtype,
    dwi.issend,
    ROUND(dwi.reportprice,4) as reportprice,
    ROUND(dwi.sendprice,4) as sendprice,
    dwi.s1,
    dwi.s2,
    dwi.gaid,
    dwi.androidid,
    dwi.idfa,
    dwi.postback,
    dwi.sendstatus,
    dwi.sendtime,
    dwi.sv,
    dwi.imei,
    dwi.imsi,
    dwi.imageid,
    dwi.affsub,
    dwi.s3,
    dwi.s4,
    dwi.s5,
    dwi.packagename,
    dwi.domain,
    dwi.respstatus,
    dwi.winprice,
    dwi.wintime,
    dwi.appprice,
    dwi.test,
    dwi.ruleid,
    dwi.smartid,
    dwi.pricepercent,
    dwi.apppercent,
    dwi.salepercent,
    dwi.appsalepercent,
    dwi.reportip,
    dwi.eventname,
    dwi.eventvalue,
    dwi.refer,
    dwi.status,
    dwi.city,
    dwi.region,
    dwi.repeated,
    dwi.l_time,
    dwi.b_date,
    dwi.b_time,

    cam.adverId       as adverid,
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
    a_ama.name        as amaaname

from ssp_overall_postback_dwi dwi
left join campaign cam      on cam.id = dwi.campaignId
left join advertiser ad     on ad.id = cam.adverId
left join offer o           on o.id = dwi.offerId
left join publisher p       on p.id = dwi.publisherId
left join app a             on a.id = dwi.subId
left join country c         on c.id = dwi.countryId
left join carrier ca        on ca.id = dwi.carrierId

left join employee p_amp    on p_amp.id = p.ampaId
left join employee a_ama    on a_ama.id = ad.amaaId;