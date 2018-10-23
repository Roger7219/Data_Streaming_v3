curr_dir=`dirname "$0"`;curr_dir=`cd "$curr_dir"; pwd`; cd $curr_dir;cd $curr_dir


# (sh update_kylin_cube.sh 0>&1 2>&1 &) > log;tail -f log
# ps -ef | grep update_kylin_cube.sh

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

        elif (( "$h" >= 3 )); then
            echo "============================UPDATE CUBE============================"

            sh sqoop_mysql_to_hive.sh

            if [ $? = 0 ];
            then
                echo "Sqoop mysql to hive finish, Will rebuild kylin cube"
                rebuildCube2Day cube_ssp_traffic_overall_dm_final
                rebuildCube2Day cube_ssp_user_keep_dm
                rebuildCube2Day cube_ssp_user_na_dm
                rebuildCube2Day cube_ssp_traffic_app_dm
                rebuildCube2Day cube_ssp_traffic_campaign_dm_final
                rebuildCube2Day cube_ssp_traffic_match_dm
                rebuildCubeALLDay cube_ssp_image_dm
                rebuildCube2Day cube_ssp_log_dm
                rebuildCube2Day cube_dsp_traffic_dm

            else
                echo "Sqoop mysql to hive fail"
            fi

        fi

        sleep $((60*5))
    done
}

function rebuildCubeALLDay(){
 cubeName="$1"

    echo "RebuildCubeAllDay: " $cubeName

    #重建全部天的cube
    startTimeString="2000-01-01 08:00:00"
    endTimeString="8000-01-01 08:00:00"

    rebuildCube "$cubeName" "$startTimeString" "$endTimeString"
}

function rebuildCube2Day(){
    cubeName="$1"

    echo "RebuildCube2Day: " $cubeName
    #重建昨天的cube
    startTimeString=`date -d "-1 day" "+%Y-%m-%d 08:00:00"`
    endTimeString=`date -d "+0 day" "+%Y-%m-%d 08:00:00"`

    rebuildCube "$cubeName" "$startTimeString" "$endTimeString"

    #重建今天的cube
    startTimeString=`date -d "0 day" "+%Y-%m-%d 08:00:00"`
    endTimeString=`date -d "+1 day" "+%Y-%m-%d 08:00:00"`

    rebuildCube "$cubeName" "$startTimeString" "$endTimeString"
}
function rebuildCube(){

    cubeName=$1
    startTimeString=$2
    endTimeString=$3

    echo "=======================================↓↓↓======================================="
    echo -e "[Trying REFRESH Kylin Cube], cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString"

    startTime=$((`date -d "$startTimeString" +%s`*1000))
    endTime=$((`date -d "$endTimeString" +%s`*1000))

    ERROR_NOT_MATCH_CUBE_SEG="does not match any existing segment in cube CUBE"
    ERROR_SEGMENTS_OVERLAP="Segments overlap"
    ERROR_EXCEPTION="exception"

    refreshResp=`curl 'http://node14:7070/kylin/api/cubes/'$cubeName'/rebuild' \
    -X PUT \
    -H 'Content-Type: application/json;charset=UTF-8' \
    -H 'Accept: application/json, text/plain, */*' \
    -H "Authorization: Basic QURNSU46S1lMSU4=" \
    -d '{
        "buildType":"REFRESH",
        "startTime":'$startTime',
        "endTime":'$endTime'
    }'`

    echo ""
    #echo "REFRESH result:  $refreshResp"

    if [[ $refreshResp =~ $ERROR_NOT_MATCH_CUBE_SEG ]]; then
        echo "REFRESH Cube Fail (NOT_MATCH_CUBE_SEG) [Will BUILD Cube] First [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"

        buildResp=`curl 'http://node14:7070/kylin/api/cubes/'$cubeName'/rebuild' \
        -X PUT \
        -H 'Content-Type: application/json;charset=UTF-8' \
        -H 'Accept: application/json, text/plain, */*' \
        -H "Authorization: Basic QURNSU46S1lMSU4=" \
        -d '{
            "buildType":"BUILD",
            "startTime":'$startTime',
            "endTime":'$endTime'
        }'`

        if [[ $buildResp =~ $ERROR_SEGMENTS_OVERLAP ]]; then
            echo "BUILD Cube Fail (SEGMENTS_OVERLAP) [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
        elif [[ $buildResp =~ $ERROR_EXCEPTION ]]; then
            echo "BUILD Cube Fail (Exception) [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
            echo -e "\nException: "$buildResp
        else
            echo "BUILD Cube OK [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
        fi

    elif [[ $refreshResp =~ $ERROR_SEGMENTS_OVERLAP ]]; then
        echo "REFRESH Cube Fail (SEGMENTS_OVERLAP) [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
    elif [[ $refreshResp =~ $ERROR_EXCEPTION ]]; then
        echo "REFRESH Cube Fail (Exception) [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
        echo -e "\nException: "$refreshResp
    else
        echo "REFRESH Cube OK [cubeName: $cubeName, startTime: $startTimeString, endTime: $endTimeString]"
    fi
    echo -e "======================================= ↑↑↑======================================="
    echo ""
    echo ""


}


main