#! /bin/bash

cd `dirname $0`

tempfifo=$$.fifo        # $$表示当前执行文件的PID

while getopts "f:tur" arg
do
        case $arg in
             f)
                group_file=$OPTARG #参数存在$OPTARG中
                ;;
             t)
                mode=$mode"test"
                ;;
             u)
                mode=$mode"update"
                ;;
			 r)
				rm -rf ../../res/group/*
				;;
             ?) 
            	echo "unkonw argument"
        exit 1
        ;;
        esac
done

trap "exec 100>&-;exec 100<&-;exit 0" 2
mkfifo $tempfifo
exec 100<>$tempfifo
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

for ((i=1; i<=3; i++))
do
    echo >&100
done

ind=0
get_jsonObj() {
	echo `cat $group_file | jq ".[$ind]"`
}


cond=`get_jsonObj`
echo $cond
while  test "$cond" != null
do
    read -u100
    {
		echo $cond
		if [ "$mode" == "test" ]; then
        	python run_analysis.py --flagfile=./conf/conf.gflag --group --test --league_cond="$cond"
		elif [ "$mode" == "update" ]; then
			python run_analysis.py --flagfile=./conf/conf.gflag --group --update --league_cond="$cond"
		elif [ "$mode" == "testupdate" -o "$mode" == "updatetest" ]; then
			python run_analysis.py --flagfile=./conf/conf.gflag --group --test --update --league_cond="$cond"
		else
			python run_analysis.py --flagfile=./conf/conf.gflag --group --league_cond="$cond"
		fi
		checkError "run_group $cond"
        echo >&100
    } & 
	ind=$(($ind+1))
	cond=`get_jsonObj`
done

wait
echo "done!!!!!!!!!!"
