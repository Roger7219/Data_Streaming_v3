curr_dir=`dirname "$0"`;curr_dir=`cd "$curr_dir"; pwd`

#------------------------------------------------------------------------------
# 程序入口
#------------------------------------------------------------------------------
function main(){

  while [ 1 = 1 ]
  do
    echo -e "\nExecute time: "`date "+%Y-%m-%d %H:%M:%S"`

    echo "调用kylin更新cube接口"
    $curr_dir/update_kylin_cube.sh

    sleep 10
  done
}