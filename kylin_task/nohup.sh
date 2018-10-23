#------------------------------------------------------------------------------
# 后台模式运行main.sh
#------------------------------------------------------------------------------
curr_dir=`dirname "$0"`;curr_dir=`cd "$curr_dir"; pwd`

function stop(){

  ps_info=`ps -ef |  grep "$1"`
  if [ "$ps_info" != "" ];
  then
    echo $ps_info | awk '{print $2}' | xargs kill
    echo -e "\n##############################################################################################"
    echo "Killed by pid: ["$ps_info"]"
    echo "##############################################################################################"
  fi
}

stop "[k]ylin_task/main.sh"

(sh $curr_dir/main.sh 1>$curr_dir/kylin_task.log 0>&1 2>&1 &)
tail -f $curr_dir/kylin_task.log
