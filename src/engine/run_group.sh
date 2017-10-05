#! /bin/bash

cd `dirname $0`

tempfifo=$$.fifo        # $$表示当前执行文件的PID
group_file=$1           # 开始时间

trap "exec 1000>&-;exec 1000<&-;exit 0" 2
mkfifo $tempfifo
exec 1000<>$tempfifo
rm -rf $tempfifo

for ((i=1; i<=8; i++))
do
    echo >&1000
done

ind=0
get_jsonObj() {
	echo `cat $group_file | jq ".[$ind]"`
}

cond=`get_jsonObj`
echo $cond
while  test "$cond" != null
do
    read -u1000
    {
		echo $cond
        python run_analysis.py --flagfile=./conf/conf.gflag --group --league_cond="$cond"
        echo >&1000
    } & 
	ind=$(($ind+1))
	cond=`get_jsonObj`
done

wait
echo "done!!!!!!!!!!"
