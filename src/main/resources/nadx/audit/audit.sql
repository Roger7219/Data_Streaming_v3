
CREATE TABLE nadx_overall_audit_dwr(
  demand_id                         int,
  crid                              string,
  ip                                string,
  country                           string,
  adm                               string,
  demand_crid_count                 int
)
PARTITIONED BY (l_time string, b_date string, b_time string, b_version STRING)
STORED AS ORC;

drop view if exists nadx_overall_audit_dm;
create view nadx_overall_audit_dm as
select * from nadx_overall_audit_dwr;