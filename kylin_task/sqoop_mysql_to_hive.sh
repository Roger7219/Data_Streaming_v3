curr_dir=`dirname "$0"`;curr_dir=`cd "$curr_dir"; pwd`

#------------------------------------------------------------------------------
# 程序入口
#------------------------------------------------------------------------------
function main(){
    sqoop_job ADVERTISER
    sqoop_job CAMPAIGN
    sqoop_job OFFER
    sqoop_job PUBLISHER

    sqoop_job APP
    sqoop_job COUNTRY
    sqoop_job CARRIER
    #sqoop_job_by_where VERSION_CONTROL "type=2"
    reimport2hive VERSION_CONTROL "type=2"
    sqoop_job EMPLOYEE
    clone_table_data EMPLOYEE ADVERTISER_AM
    sqoop_job IMAGE_INFO
}

function clone_table_data() {
 beeline -u jdbc:hive2://node17:10000/default \
    --hiveconf mapred.map.tasks=60 \
    --hiveconf mapred.min.split.size=1000000000 \
    --hiveconf mapred.max.split.size=2000000000 \
    --hiveconf mapred.min.split.size.per.node=1000000000 \
    --hiveconf mapred.min.split.size.per.rack=1000000000 \
    --hiveconf mapreduce.job.queuename=queueSqoop \
    -e "INSERT OVERWRITE TABLE $2 SELECT * FROM $1"

}
function delete_hive_table_and_reset_job(){

    beeline -u jdbc:hive2://node17:10000/default -e "drop table ADVERTISER"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table CAMPAIGN"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table OFFER"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table PUBLISHER"

    beeline -u jdbc:hive2://node17:10000/default -e "drop table APP"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table COUNTRY"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table CARRIER"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table VERSION_CONTROL"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table EMPLOYEE"
    beeline -u jdbc:hive2://node17:10000/default -e "drop table IMAGE_INFO"


    echo "======================================="
    echo "Delete Sqoop Jobs ..."
    echo "======================================="

    sqoop job -delete ADVERTISER_job
    sqoop job -delete CAMPAIGN_job
    sqoop job -delete OFFER_job
    sqoop job -delete PUBLISHER_job

    sqoop job -delete APP_job
    sqoop job -delete COUNTRY_job
    sqoop job -delete CARRIER_job
    sqoop job -delete VERSION_CONTROL_job
    sqoop job -delete EMPLOYEE_job
    sqoop job -delete IMAGE_INFO_job

}
#version_control
function check_duplicate(){
    table=$1
    key=$2
    maxCount=$((`beeline -u jdbc:hive2://node16:10016,node15:10016/default --outputformat=table \
    --hiveconf mapreduce.job.queuename=queueSqoop \
    -e "select count(1) from $table group by  $key order by 1 desc limit 1" | \
    sed -n '4p' | \
    sed 's/[ \t|]*//g' `))

    echo "maxCount: $maxCount"

    if [ $maxCount -gt 1 ]; then
        return 1
    else
        return 0
    fi
}

function sqoop_job(){
	echo "建hive表：$1"
    sqoop create-hive-table \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-table $1 \
    --fields-terminated-by '\001' \
    --lines-terminated-by "\n"
    
    #echo "mysql导入数据到hive"
    #sqoop import -D mapred.job.queue.name=queueSqoop \
    #--connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    #--username root \
    #--password '@dfei$@DCcsYG' \
    #--table $1 \
    #--hive-import \
    #--hive-table $1
    #--incremental append \
    #--check-column id \
    #--last-value 0
    
    echo "建job：mysql $1导入数据到hive $1"
    sqoop job -D mapred.job.queue.name=queueSqoop \
    --create $1_job \
    -- import \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-import \
    --hive-table $1 \
    --hive-drop-import-delims \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --fields-terminated-by '\001' \
    --incremental append \
    --check-column id \
    --last-value 0
    
    #sqoop job --delete country_job
    echo "执行job：mysql $1导入数据到hive $1"
    sqoop job --exec $1_job
    echo "完成job：mysql $1导入数据到hive $1"

    echo "===============check_duplicate start==============="
    check_duplicate $1 "id"
    is_dup=$?

    #check
    if [ $is_dup -eq 1 ]; then
        echo "REST JOB of table $1 !!!!!!"
        beeline -u jdbc:hive2://node17:10000/default -e "drop table $1"
        sqoop job -delete "$1_job"
        sqoop_job $1
    fi
    echo "===============check_duplicate end==============="

}

function sqoop_job_by_where(){
    echo "建hive表：$1"
    sqoop create-hive-table \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-table $1 \
    --fields-terminated-by '\001' \
    --lines-terminated-by "\n"
    
    #echo "mysql导入数据到hive"
    #sqoop import -D mapred.job.queue.name=queueSqoop \
    #--connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    #--username root \
    #--password '@dfei$@DCcsYG' \
    #--table $1 \
    #--hive-import \
    #--hive-table $1 \
    #--where "$2" \
    #--incremental append \
    #--check-column id \
    #--last-value 0
    
    echo "建job：mysql $1导入数据到hive $1"
    sqoop job -D mapred.job.queue.name=queueSqoop \
    --create $1_job \
    -- import \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-import \
    --hive-table $1 \
    --where "$2" \
    --hive-drop-import-delims \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --fields-terminated-by '\001' \
    --incremental append \
    --check-column id \
    --last-value 0
    
    #sqoop job --delete country_job
    echo "执行job：mysql $1导入数据到hive $1"
    sqoop job --exec $1_job
    echo "完成job：mysql $1导入数据到hive $1"
}

function reimport2hive(){

    #echo "Delete hive表：$1 ..."
    #beeline -u jdbc:hive2://node17:10000/default -e "drop table $1"

    echo "执行建hive表：$1 ..."
    sqoop create-hive-table \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-table $1 \
    --fields-terminated-by '\001' \
    --lines-terminated-by "\n"

    echo "开始mysql $1导入数据到hive $1"
    sqoop import -D mapred.job.queue.name=queueSqoop \
    --connect jdbc:mysql://104.250.132.218:8905/kok_ssp \
    --username root \
    --password '@dfei$@DCcsYG' \
    --table $1 \
    --hive-import \
    --hive-overwrite \
    --hive-table $1 \
    --where "$2" \
    --hive-drop-import-delims \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --fields-terminated-by '\001'

    echo "完成mysql $1导入数据到hive $1"
}

main







### select * from ADVERTISER limit 10;
### select * from CAMPAIGN limit 10;
### select * from OFFER limit 10;
### select * from PUBLISHER limit 10;
### select * from APP limit 10;
### select * from COUNTRY limit 10;
### select * from CARRIER limit 10;
### select * from VERSION_CONTROL limit 10;
### select * from EMPLOYEE limit 10;


### drop table ADVERTISER
### drop table CAMPAIGN;
### drop table OFFER;
### drop table PUBLISHER;
### drop table APP;
### drop table COUNTRY;
### drop table CARRIER;
### drop table EMPLOYEE;

### sqoop job -delete ADVERTISER_job
### sqoop job -delete OFFER_job



 
 

