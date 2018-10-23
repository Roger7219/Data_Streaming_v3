curr_dir=`dirname "$0"`;curr_dir=`cd "$curr_dir"; pwd`; cd $curr_dir;cd $curr_dir


# (sh clean_kylin_while.sh 0>&1 2>&1 &) > log;tail -f log
# ps -ef | grep clean_kylin_while.sh

function main(){

    while [ 1 = 1 ];
    do

        echo "Curr pid: " $$

        h=`date --date='0 days ago' "+%-H"`

        if (( "0" <= "$h" && "$h" <= "1" )); then
            echo "==============================STOPING=============================="

        elif (( "2" <= "$h" && "$h" < "3" )); then
            echo "==============================CLEAN================================"
            /root/kairenlo/apache-kylin-1.6.0-hbase1.x-bin/bin/kylin.sh org.apache.kylin.storage.hbase.util.StorageCleanupJob --delete true
        fi

        sleep $((60*5))
    done
}


main