CREATE VIEW `ssp_top_offer_dm` AS select
    `t`.`category`,
    `t`.`type`,
    `t`.`countryid`,
    `t`.`carrierid`,
    `t`.`offerid`,
    `t`.`sendcount`,
    `t`.`clickcount`,
    `t`.`showcount`,
    `t`.`fee`,
    `t`.`totalsendcount`,
    `t`.`totalclickcount`,
    `t`.`totalshowcount`,
    `t`.`totalfee`,
    `t`.`feecount`,
    `t`.`totalfeecount`,
    `t`.`statdate`  as `b_date`,
    `c`.`name` as `countryname`,
    `ca`.`name` as `carriername`,
    nvl(`o`.`name`,'-') `offername`,
    CASE `o`.`status` WHEN 0 THEN 'Disabled' WHEN 1 THEN 'Enabled' END as `offerstatus`,
    CASE `o`.`amstatus` WHEN 0 THEN 'Pending' WHEN 1 THEN 'Approved' WHEN 2 THEN 'Deny' END as `offeramstatus`,
    CASE `o`.`isapi` WHEN 0 THEN 'NO' WHEN 1 THEN 'IS' END AS `isapistring`
from `default`.`top_offer_dwr` `t`
LEFT JOIN `default`.`carrier` `CA`        on `ca`.`id` = `t`.`carrierid`
LEFT JOIN `default`.`offer` `O`           on `o`.`id` = `t`.`offerid`
LEFT JOIN `default`.`country` `C`         on `c`.`id` = `t`.`countryid`
where `t`.`statdate` >= "2017-12-18"
union  all
select
    `t1`.`category`,
    `t1`.`type`,
    `t1`.`countryid`,
    `t1`.`carrierid`,
    `t1`.`offerid`,
    `t1`.`sendcount`,
    `t1`.`clickcount`,
    `t1`.`showcount`,
    `t1`.`fee`,
    `t1`.`totalsendcount`,
    `t1`.`totalclickcount`,
    `t1`.`totalshowcount`,
    `t1`.`totalfee`,
    `t1`.`feecount`,
    `t1`.`totalfeecount`,
    `t1`.`statdate`  as `b_date`,
    `c`.`name` as `countryname`,
    `ca`.`name` as `carriername`,
    nvl(`o`.`name`,'-') `offername`,
    CASE `o`.`status` WHEN 0 THEN 'Disabled' WHEN 1 THEN 'Enabled' END as `offerstatus`,
    CASE `o`.`amstatus` WHEN 0 THEN 'Pending' WHEN 1 THEN 'Approved' WHEN 2 THEN 'Deny' END as `offeramstatus`,
    CASE `o`.`isapi` WHEN 0 THEN 'NO' WHEN 1 THEN 'IS' END AS `isapistring`
from `default`.`top_offer` `t1`
LEFT JOIN `default`.`carrier` `CA`        on `ca`.`id` = `t1`.`carrierid`
LEFT JOIN `default`.`offer` `O`           on `o`.`id` = `t1`.`offerid`
LEFT JOIN `default`.`country` `C`         on `c`.`id` = `t1`.`countryid`
where `t1`.`statdate` < "2017-12-18"