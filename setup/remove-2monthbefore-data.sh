#!/usr/bin/bash

#current date

currDate="`date +%Y-%m-%d`"
echo $currDate

#get before 60days date
#lastDate = $(date -d "$currDate -60 days")
v_two_month_day=$(date -d "$currDate -1 month" +%Y-%m-%d)

echo $v_two_month_day

#The table that would be drop data

#tables=$1
tables=$@

echo "tables: $tables"

#drop 2months before date in table
for table in $tables
do
echo -e "table list :$table \n"

beeline -u jdbc:hive2://node17:10000/default --outputformat=vertical --hivevar table=$table --hivevar b_date=${v_two_month_day} -e 'alter table ${table} drop partition(b_date<"${b_date}")'

done


