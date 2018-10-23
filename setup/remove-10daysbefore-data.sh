#!/usr/bin/bash
#get before 10days date
tenBeforeDate=`date -d '5 days ago' +%Y-%m-%d`

echo -e "tenBeforeDate : $tenBeforeDate \n"

#The table that would be drop data
# ssp_overall_fill_dwi ssp_overall_send_dwi ssp_overall_show_dwi ssp_overall_click_dwi ssp_overall_win_dwi
tables=$@

#drop 10days before data in table
for table in $tables
do
echo -e "table list :$table \n"

beeline -u jdbc:hive2://node17:10000/default --outputformat=vertical --hivevar table=$table --hivevar b_date=${tenBeforeDate} -e 'alter table ${table} drop partition(b_date<"${b_date}")'

done


