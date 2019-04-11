#!/usr/bin/bash

### crontab setting
# 0 5 * * * /usr/bin/bash /root/remove-hive-data.sh nadx_traffic_dwi nadx_overall_traffic_dwi

### script
#get before 10days date
tenBeforeDate=`date -d '5 days ago' +%Y-%m-%d`

echo -e "tenBeforeDate : $tenBeforeDate \n"

#The table that would be drop data
tables=$@

#drop 5days before data in table
for table in $tables
do
echo -e "table list :$table \n"

beeline -u jdbc:hive2://master:10000/default --outputformat=vertical --hivevar table=$table --hivevar b_date=${tenBeforeDate} -e 'alter table ${table} drop partition(b_date<="${b_date}")'

done