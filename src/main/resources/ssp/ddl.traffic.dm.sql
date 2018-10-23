
create view ssp_traffic_dwr(
    publisherId  INT,
    subId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    adType       INT,
    campaignId   INT,
    offerId      Int,
    reportCount        BIGINT,   --计费条数
    sendCount    DECIMAL(19,10), --计费显示条数
    reportPrice  DECIMAL(19,10), --计费金额
    sendPrice    DECIMAL(19,10), --计费显示金额
    requestCount BIGINT,         --请求数
    sendCount    BIGINT,         --下发数
    showCount    BIGINT,         --展示数
    clickCount   BIGINT,         --点击数
    conversion BIGINT,      --转化数
    userCount   INT,        --新增用户数
    activeCount INT         --活跃用户数
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;

--维表
create table ad_type(
   id   INT,
   name STRING
)
STORED AS ORC;
insert overwrite table ad_type
select 2, "Mobile-320×50" from dual
union all
select 3, "Mobile-300×250" from dual
union all
select 4, "Interstitial" from dual
union all
select 5, "Mobile-320×480" from dual
union all
select 6, "Mobile-1200*627" from dual
union all
select 7, "AdType-Id-7" from dual
union all
select 8, "AdType-Id-8" from dual;



-- For Kylin
-- app use publisher am
-- overall use publisher am
-- campaign use advertiser am (广告主的)
CREATE VIEW publisher_am AS
SELECT * FROM employee;
CREATE VIEW advertiser_am AS
SELECT * FROM employee;

CREATE TABLE advertiser_am LIKE employee;
INSERT OVERWRITE TABLE advertiser_am
SELECT * FROM employee;



--EMPLOYEE e on  e.id =  p.amid

create view ssp_traffic_overall_dm as
select
    publisherId,    --INT,
    p.amId as pAmId, -- amId 对应 employee.id
    ad.amId as aAmId,
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
    cast(null as bigint) as requestCount,
    cast(null as bigint) as sendCount,
    cast(null as bigint) as showCount,
    c.times as clickCount,
    cast(null as bigint) as feeReportCount,          --计费条数
    cast(null as bigint) as feeSendCount,            --计费显示条数
    cast(null as decimal(19,10)) as feeReportPrice,          --计费金额(真实收益)
    cast(null as decimal(19,10)) as feeSendPrice,            --计费显示金额(收益)
    c.cpcBidPrice,
    cast(null as decimal(19,10)) as cpmBidPrice,
    cast(null as bigint) as conversion, --转化数，不含展示和点击产生的
    c.cpcTimes as allConversion,        --转化数，含展示和点击产生的
    c.cpcSendPrice as revenue,     --收益
    c.cpcBidPrice as realRevenue, --真实收益
    cast(null as bigint) as userCount,        --BIGINT,
    cast(null as bigint) as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date   -- STRING
from ssp_click_dwr c
left join campaign ca on c.campaignId = ca.id
left join advertiser ad on ad.id = ca.adverid
left join publisher p on p.id = c.publisherId
left join version_control v on v.version = c.sv
union all
select
    publisherId,  --INT,
    p.amId as pAmId,
    ad.amId as aAmId,
    subId as appId,        --INT,
    f.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
    adType,       --INT,
    campaignId,   --INT,
    offerId,      --Int,
    ca.adCategory1,  --关联campaign.adCategory1
    ca.adVerid,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    f.times       as feeReportCount, --计费条数
    f.sendTimes   as feeSendCount,   --计费显示条数
    f.reportPrice as feeReportPrice, --计费金额(真实收益)
    f.sendPrice   as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    f.times as conversion,        --转化数，目前不要含展示和点击产生的
    f.times as allConversion,     --转化数，含展示和点击产生的
    f.sendPrice as revenue,       --收益
    f.reportPrice as realRevenue, --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_fee_dwr f
left join campaign ca on f.campaignId = ca.id
left join advertiser ad on ad.id = ca.adverid
left join publisher p on  p.id = f.publisherId
left join version_control v on v.version = f.sv
union all
select
    publisherId,  --INT,
    p.amId as pAmId,
    ad.amId as aAmId,
    subId as appId,        --INT,
    s.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
    adType,       --INT,
    campaignId,   --INT,
    offerId,      --Int,
    ca.adCategory1,  --关联campaign.adCategory1
    ca.adVerid,
    null as requestCount,
    s.times as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,    --转化数，目前不要含展示和点击产生的
    null as allConversion, --转化数，含展示和点击产生的
    null as revenue,       --收益
    null as realRevenue,   --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_send_dwr s
left join campaign ca on s.campaignId = ca.id
left join advertiser ad on ad.id = ca.adverid
left join publisher p on  p.id =  s.publisherId
left join version_control v on v.version = s.sv
-- and v.type = 2
union all
select
    publisherId,    --INT,
    p.amId as pAmId,
    ad.amId as aAmId,
    subId as appId, --INT,
    w.countryId,      --INT,
    carrierId,      --INT,
    v.id as versionId,  -- INT
    adType,         --INT,
    campaignId,     --INT,
    offerId,        --Int,
    ca.adCategory1,  --关联campaign.adCategory1
    ca.adVerid,
    null as requestCount,
    null as sendCount,
    w.times as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    w.cpmBidPrice as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    w.cpmTimes as allConversion,    --转化数，含展示和点击产生的
    w.cpmSendPrice as revenue,        --收益
    w.cpmBidPrice as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_show_dwr w
left join campaign ca on ca.id = w.campaignId
left join advertiser ad on ad.id = ca.adverid
left join publisher p on  p.id =  w.publisherId
left join version_control v on v.version = w.sv;

-- =======================
create view ssp_traffic_app_dm as
select
    publisherId,    --INT,
    p.amId,         -- amId 对应 employee.id
    subId as appId, --INT,
    c.countryId,    --INT,
    carrierId,    --INT,
    -- sv,         --STRING,
    v.id as versionId,      -- INT
--    adType as adFormatId,   --INT, (adFormatId对应offer.adtype,逗号分隔，需展开)
--    campaignId,     --INT,
--    offerId,        --Int,
--    ca.adCategory1, --INT 关联campaign.adCategory1
--    ca.adVerid,
    cast(null as bigint) as requestCount,
    cast(null as bigint) as sendCount,
    cast(null as bigint) as showCount,
    c.times as clickCount,
    cast(null as bigint) as feeReportCount,          --计费条数
    cast(null as bigint) as feeSendCount,            --计费显示条数
    cast(null as decimal(19,10)) as feeReportPrice,          --计费金额(真实收益)
    cast(null as decimal(19,10)) as feeSendPrice,            --计费显示金额(收益)
    c.cpcBidPrice,
    cast(null as decimal(19,10)) as cpmBidPrice,
    cast(null as bigint) as conversion, --转化数，不含展示和点击产生的
    c.cpcTimes as allConversion,        --转化数，含展示和点击产生的
    c.cpcSendPrice as revenue,     --收益
    c.cpcBidPrice as realRevenue, --真实收益
    cast(null as bigint) as userCount,        --BIGINT,
    cast(null as bigint) as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date   -- STRING
from ssp_click_dwr c
left join campaign ca on c.campaignId = ca.id
left join publisher p on p.id = c.publisherId
left join version_control v on v.version = c.sv
union all
select
    publisherId,  --INT,
    p.amId,
    subId as appId,        --INT,
    f.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
--    adType,       --INT,
--    campaignId,   --INT,
--    offerId,      --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    f.times       as feeReportCount, --计费条数
    f.sendTimes   as feeSendCount,   --计费显示条数
    f.reportPrice as feeReportPrice, --计费金额(真实收益)
    f.sendPrice   as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    f.times as conversion,        --转化数，目前不要含展示和点击产生的
    f.times as allConversion,     --转化数，含展示和点击产生的
    f.sendPrice as revenue,       --收益
    f.reportPrice as realRevenue, --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_fee_dwr f
left join campaign ca on f.campaignId = ca.id
left join publisher p on p.id = f.publisherId
left join version_control v on v.version = f.sv
union all
select
    publisherId,  --INT,
    p.amId,
    subId as appId,        --INT,
    s.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
--    adType,       --INT,
--    campaignId,   --INT,
--    offerId,      --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    s.times as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,    --转化数，目前不要含展示和点击产生的
    null as allConversion, --转化数，含展示和点击产生的
    null as revenue,       --收益
    null as realRevenue,   --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_send_dwr s
left join campaign ca on s.campaignId = ca.id
left join publisher p on  p.id =  s.publisherId
left join version_control v on v.version = s.sv
-- and v.type = 2
union all
select
    publisherId,    --INT,
    p.amId,
    subId as appId, --INT,
    w.countryId,      --INT,
    carrierId,      --INT,
    v.id as versionId,  -- INT
--    adType,         --INT,
--    campaignId,     --INT,
--    offerId,        --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    null as sendCount,
    w.times as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    w.cpmBidPrice as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    w.cpmTimes as allConversion,    --转化数，含展示和点击产生的
    w.cpmSendPrice as revenue,        --收益
    w.cpmBidPrice as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_show_dwr w
left join campaign ca on ca.id = w.campaignId
left join publisher p on  p.id =  w.publisherId
left join version_control v on v.version = w.sv
union all
select
    publisherId,    --INT,
    p.amId,
    subId as appId, --INT,
    fi.countryId,      --INT,
    carrierId,      --INT,
    v.id as versionId,  -- INT
--    adType,         --INT,
--    null as campaignId,     --INT,
--    null as offerId,        --Int,
--    null as adCategory1,    --关联campaign.adCategory1
--    null as adVerid,
    fi.times as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    null as allConversion,  --转化数，含展示和点击产生的
    null as revenue,        --收益
    null as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date   -- STRING
from ssp_fill_dwr fi
left join publisher p on  p.id = fi.publisherId
left join version_control v on v.version = fi.sv
union all
select
    a.publisherId,
    p.amId,
    appId,              -- INT,
    un.countryId,       -- INT,
    carrierId,          -- INT,
    v.id as versionId,  -- INT
--    null as adType,
--    null as campaignId,
--    null as offerId,
--    null as adCategory1,
--    null as adVerid,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount,  --计费条数
    null as feeSendCount,    --计费显示条数
    null as feeReportPrice,  --计费金额(真实收益)
    null as feeSendPrice,    --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,   --转化数，目前不要含展示和点击产生的
    null as allConversion,--转化数，含展示和点击产生的
    null as revenue,        --收益
    null as realRevenue,    --真实收益
    newCount as userCount,  -- BIGINT,
    null as activeCount,            -- BIGINT
    l_time,
    b_date
--from ssp_user_new_and_active_dwr na
from ssp_user_new_dwr un
left join app a on a.id  = un.appid
left join publisher p on  p.id =  a.publisherid
left join version_control v on v.version = un.sv
union all
select
    a.publisherId,
    p.amId,
    appId,              -- INT,
    ua.countryId,       -- INT,
    carrierId,          -- INT,
    v.id as versionId,  -- INT
--    null as adType,
--    null as campaignId,
--    null as offerId,
--    null as adCategory1,
--    null as adVerid,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount,  --计费条数
    null as feeSendCount,    --计费显示条数
    null as feeReportPrice,  --计费金额(真实收益)
    null as feeSendPrice,    --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,   --转化数，目前不要含展示和点击产生的
    null as allConversion,--转化数，含展示和点击产生的
    null as revenue,        --收益
    null as realRevenue,    --真实收益
    null as userCount,  -- BIGINT,
    activeCount,            -- BIGINT
    l_time,
    b_date
--from ssp_user_new_and_active_dwr na
from ssp_user_active_dwr ua
left join app a on a.id  = ua.appid
left join publisher p on  p.id =  a.publisherid
left join version_control v on v.version = ua.sv;



-- =======================
create view ssp_traffic_match_dm as
select
    publisherId,    --INT,
    p.amId,         -- amId 对应 employee.id
    subId as appId, --INT,
    c.countryId,    --INT,
    carrierId,    --INT,
    -- sv,         --STRING,
    v.id as versionId,      -- INT
    adType as adFormatId,   --INT, (adFormatId对应offer.adtype,逗号分隔，需展开)
--    campaignId,     --INT,
--    offerId,        --Int,
--    ca.adCategory1, --INT 关联campaign.adCategory1
--    ca.adVerid,
    cast(null as bigint) as requestCount,
    cast(null as bigint) as sendCount,
    cast(null as bigint) as showCount,
    c.times as clickCount,
    cast(null as bigint) as feeReportCount,          --计费条数
    cast(null as bigint) as feeSendCount,            --计费显示条数
    cast(null as decimal(19,10)) as feeReportPrice,          --计费金额(真实收益)
    cast(null as decimal(19,10)) as feeSendPrice,            --计费显示金额(收益)
    c.cpcBidPrice,
    cast(null as decimal(19,10)) as cpmBidPrice,
    cast(null as bigint) as conversion, --转化数，不含展示和点击产生的
    c.cpcTimes as allConversion,        --转化数，含展示和点击产生的
    c.cpcSendPrice as revenue,     --收益
    c.cpcBidPrice as realRevenue, --真实收益
    cast(null as bigint) as userCount,        --BIGINT,
    cast(null as bigint) as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date   -- STRING
from ssp_click_dwr c
left join campaign ca on c.campaignId = ca.id
left join publisher p on p.id = c.publisherId
left join version_control v on v.version = c.sv
union all
select
    publisherId,  --INT,
    p.amId,
    subId as appId,        --INT,
    f.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
    adType,       --INT,
--    campaignId,   --INT,
--    offerId,      --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    f.times       as feeReportCount, --计费条数
    f.sendTimes   as feeSendCount,   --计费显示条数
    f.reportPrice as feeReportPrice, --计费金额(真实收益)
    f.sendPrice   as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    f.times as conversion,        --转化数，目前不要含展示和点击产生的
    f.times as allConversion,     --转化数，含展示和点击产生的
    f.sendPrice as revenue,       --收益
    f.reportPrice as realRevenue, --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_fee_dwr f
left join campaign ca on f.campaignId = ca.id
left join publisher p on p.id = f.publisherId
left join version_control v on v.version = f.sv
union all
select
    publisherId,  --INT,
    p.amId,
    subId as appId,        --INT,
    s.countryId,    --INT,
    carrierId,    --INT,
    v.id as versionId,  -- INT
    adType,       --INT,
--    campaignId,   --INT,
--    offerId,      --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    s.times as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,    --转化数，目前不要含展示和点击产生的
    null as allConversion, --转化数，含展示和点击产生的
    null as revenue,       --收益
    null as realRevenue,   --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_send_dwr s
left join campaign ca on s.campaignId = ca.id
left join publisher p on  p.id =  s.publisherId
left join version_control v on v.version = s.sv
-- and v.type = 2
union all
select
    publisherId,    --INT,
    p.amId,
    subId as appId, --INT,
    w.countryId,      --INT,
    carrierId,      --INT,
    v.id as versionId,  -- INT
    adType,         --INT,
--    campaignId,     --INT,
--    offerId,        --Int,
--    ca.adCategory1,  --关联campaign.adCategory1
--    ca.adVerid,
    null as requestCount,
    null as sendCount,
    w.times as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    w.cpmBidPrice as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    w.cpmTimes as allConversion,    --转化数，含展示和点击产生的
    w.cpmSendPrice as revenue,        --收益
    w.cpmBidPrice as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_show_dwr w
left join campaign ca on ca.id = w.campaignId
left join publisher p on  p.id =  w.publisherId
left join version_control v on v.version = w.sv
union all
select
    publisherId,    --INT,
    p.amId,
    subId as appId, --INT,
    fi.countryId,      --INT,
    carrierId,      --INT,
    v.id as versionId,  -- INT
    adType,         --INT,
--    null as campaignId,     --INT,
--    null as offerId,        --Int,
--    null as adCategory1,    --关联campaign.adCategory1
--    null as adVerid,
    fi.times as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    null as allConversion,  --转化数，含展示和点击产生的
    null as revenue,        --收益
    null as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date   -- STRING
from ssp_fill_dwr fi
left join publisher p on  p.id = fi.publisherId
left join version_control v on v.version = fi.sv;











-------------------------------------------------------------------------------------------
--
--    publisherId  INT,
--    subId        INT,
--    countryId    INT,
--    carrierId    INT,
--    sv           STRING,
--    adType       INT,
--    times        BIGINT


































-----------------
create view test_view1 as
select
    a.publisherId,
    appId,      --  INT,
    na.countryId,    --  INT,
    carrierId,     --  INT,
    sv,           --  STRING,
    null as adType,
    null as campaignId,
    null as offerId,
    null as adCategory1,
    null as requestCount,
    null as sendCount,
    null as showCount,
    null as clickCount,
    null as feeReportCount,  --计费条数
    null as feeSendCount,    --计费显示条数
    null as feeReportPrice,  --计费金额(真实收益)
    null as feeSendPrice,    --计费显示金额(收益)
    null as cpcBidPrice,
    null as cpmBidPrice,
    null as conversion,   --转化数，目前不要含展示和点击产生的
    null as allConversion,--转化数，含展示和点击产生的
    null as revenue,        --收益
    null as realRevenue,    --真实收益
    newCount as userCount,  -- BIGINT,
    activeCount,             -- BIGINT
    l_time,
    b_date
from ssp_user_new_and_active_dwr na
left join app a on a.id  = na.appid
left join publisher p on  p.id =  a.publisherId



create view test_view2 as
select
    publisherId,    --INT,
    subId as appId, --INT,
    countryId,      --INT,
    carrierId,      --INT,
    sv,             --STRING,
    adType,         --INT,
    campaignId,     --INT,
    offerId,        --Int,
    ca.adCategory1,  --关联campaign.adCategory1
    null as requestCount,
    null as sendCount,
    w.times as showCount,
    null as clickCount,
    null as feeReportCount, --计费条数
    null as feeSendCount,   --计费显示条数
    null as feeReportPrice, --计费金额(真实收益)
    null as feeSendPrice,   --计费显示金额(收益)
    null as cpcBidPrice,
    w.cpmBidPrice as cpmBidPrice,
    null as conversion,             --转化数，目前不要含展示和点击产生的
    w.cpmTimes as allConversion,    --转化数，含展示和点击产生的
    w.cpmBidPrice as revenue,        --收益
    w.cpmBidPrice as realRevenue,    --真实收益
    null as userCount,        --BIGINT,
    null as activeCount,      --BIGINT
    l_time, -- STRING,
    b_date -- STRING
from ssp_show_dwr w
left join campaign ca on w.campaignId = ca.id



(
--create table ssp_user_active_dwr(
    appId        INT,
    countryId    INT,
    carrierId    INT,
    sv           STRING,
    newCount     BIGINT,
    activeCount  BIGINT
)
PARTITIONED BY (l_time STRING, b_date STRING)
STORED AS ORC;


--
select
    publisherId,  --INT,
    subId,        --INT,
    countryId,    --INT,
    carrierId,    --INT,
    sv,           --STRING,
    adType,       --INT,
    campaignId,   --INT,
    offerId,      --Int,
    null as reportCount,
    null as sendCount,
    c.bidPrice as reportPrice,
    null as sendPrice,
    null as requestCount,
    null as sendCount,
    null as showCount,
    c.times as clickCount,
    null as conversion,
    null as userCount,        --BIGINT,
    null as activeCount     --DECIMAL(19,10)
from ssp_fill_dwr c