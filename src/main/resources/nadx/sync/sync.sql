
-- Hive
drop table tb_demand_account;
 CREATE TABLE tb_demand_account(
   id                            int,
   tb_user_id                    int,
   bd_id                         int,
   am_id                         int,
   name                          string,
   status                        int,
   system_name                   string,
   target_source_token_banner    string,
   target_source_token_native    string,
   target_source_token_video     string,
   target_source_token_audio     string,
   protocol                      int,
   currency                      string,
   balance                       double,
   rtb_version                   string,
   markup_method                 int,
   ad_format                     int,
   secure                        int,
   gzip                          int,
   price_model                   string,
   floor                         double,
   auction_type                  int,
   second_price_subtract_percent string,
   second_price_delta            double,
   notify_price_add_percent      int,
   country                       string,
   country_direction             int,
   os                            int,
   max_qps                       int,
   daily_request_cap             int,
   ip                            string,
   ip_direction                  int,
   publisher                     string,
   publisher_direction           int,
   domain                        string,
   domain_direction              int,
   cat_l1                        string,
   cat                           string,
   cat_direction                 int,
   bundle                        string,
   bundle_direction              int,
   sizes                         string,
   sizes_direction               int,
   device_type                   string,
   media_type                    string,
   owner_domain_probability      double,
   owner_bundle_probability      double,
   owner_publisher_probability   double,
   floor_percent                 int,
   last_updated_time             string,
   black_white_filter            string,
   filter_type                   int,
   note                          string,
   level                         int
)
STORED AS ORC;


-- Hive
drop table tb_supply_account;
CREATE TABLE tb_supply_account (
  id                    int      ,
  tb_user_id            int      ,
  bd_id                 int      ,
  am_id                 int      ,
  token                 string   ,
  name                  string   ,
  status                int      ,
  system_name           string   ,
  traffic_type          int      ,
  protocol              int      ,
  profit_percent        double      ,
  price_model           string   ,
  currency              string   ,
  floor                 double   ,
  decision_source       int      ,
  secure                int      ,
  company               string   ,
  domain                string   ,
  country               string   ,
  address               string   ,
  phone                 string   ,
  skype                 string   ,
  wechat                string   ,
  max_qps               int      ,
  daily_request_cap     int      ,
  adomains              string   ,
  cids                  string   ,
  crids                 string   ,
  floor_percent         int   ,
  last_updated_time     string
  )
  STORED AS ORC;
--where b_date <'2017-09-10';


select count(1) from tb_supply_account;
select count(1) from tb_demand_account;

select * from tb_supply_account limit 1 ;
select * from tb_demand_account limit 1 ;


select max(last_updated_time) from tb_supply_account;