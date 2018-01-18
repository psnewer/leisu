#! /bin/bash

cd `dirname $0`

tempfifo=$$.fifo        # $$表示当前执行文件的PID
group_file=$1           # 开始时间

trap "exec 1000>&-;exec 1000<&-;exit 0" 2
mkfifo $tempfifo
exec 1000<>$tempfifo
rm -rf $tempfifo

function checkError()
{
    if [ $? -eq 0 ]
    then
        echo "$1 success"
    else
        echo "$1 failed"
        exit 0
    fi
}

for ((i=1; i<=30; i++))
do
    echo >&1000
done

ind=0
get_jsonObj() {
	echo `cat $group_file | jq ".[$ind]"`
}

#rm -rf /Users/miller/Documents/workspace/leisu/res/group/*

cond=`get_jsonObj`
echo $cond
while  test "$cond" != null
do
    read -u1000
    {
		echo $cond
        python run_analysis.py --flagfile=./conf/conf.gflag --group --test --league_cond="$cond"
		checkError "run_group $cond"
        echo >&1000
    } & 
	ind=$(($ind+1))
	cond=`get_jsonObj`
done

wait
echo "done!!!!!!!!!!"
