

beeline -u jdbc:hive2://node17:10000/default --outputformat=vertical -e 'alter table ssp_fill_dwi drop partition("b_date=2017-07-21")'