startTimeString="2000-01-01 08:00:00"
endTimeString="2000-01-02 08:00:00"

startTime=$((`date -d "$startTimeString" +%s`*1000))
endTime=$((`date -d "$endTimeString" +%s`*1000))


cubeName="cube_ssp_app_dm"

curl 'http://node14:7070/kylin/api/cubes/'$cubeName'/rebuild' \
        -X PUT \
        -H 'Content-Type: application/json;charset=UTF-8' \
        -H 'Accept: application/json, text/plain, */*' \
        -H "Authorization: Basic QURNSU46S1lMSU4=" \
        -d '{
            "buildType":"BUILD",
            "startTime":'$startTime',
            "endTime":'$endTime'
        }'