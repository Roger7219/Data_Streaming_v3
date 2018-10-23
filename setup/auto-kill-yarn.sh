# for i in `ps -aux | sort -k4nr | head -n 3 | awk '{print $2}'`;do
#  echo $i
#  break;
# done
#
#
#
#
# ps -u yarn -aux | sort -k1nr  -k4n  | head -n 2 | awk '{print $2}'
#
#
#
# ps -u yarn  -o pmem=,pid,lstart,user | sort -k1nr | awk '{print $2}' | head -n 2

# crontab 配置
# * * * * * sleep 20; /root/auto-kill-yarn.sh >> /root/auto-kill-yarn.log 2>&1


# 当机器可用内存小于指定内存时，kill掉占用内存最高的2个yarn 进程
#limitG=6
#for i in `free -g | awk '{print $7}'`;do
#  if [ $i -lt $limitG ] ; then
#
#    forKill=`ps -u yarn  -o pmem=,pid,lstart,user | sort -k3nr -k1nr | awk '{print $2}' | head -n 2`
#    echo -e "The memory is less than ${limitG}G and is about to kill the yarn process:\n $forKill"
#    for j in $forKill; do
#        kill $j
#    done
#  fi
#  echo "Done time: "`date "+%Y-%m-%d %H:%M:%S" `
#  break;
#done


limitG=6
for i in `free -g | awk '{print $7}'`;do
  if [ $i -lt $limitG ] ; then

    forKill=`ps -u yarn -o lstart,pmem=,pid,user --no-headers|awk '{
       cmd="date -d\""$1 FS $2 FS $3 FS $4 FS $5"\" +\047%Y-%m-%dT%H:%M:%S\047";
       cmd | getline d; close(cmd); $1=$2=$3=$4=$5="-"; printf "%s\n",d$0
       }' | sort -k1,6 -nr | awk '{print $7}' | head -n 2`

    echo -e "The memory is less than ${limitG}G and is about to kill the yarn process:\n $forKill"
    for j in $forKill; do
        kill $j
    done
  fi
  echo "Done time: "`date "+%Y-%m-%d %H:%M:%S" `
  break;
done



# echo -e "b 1\na 2\nb 3\nc 2\na 1" |   sort -k1,2 -nr