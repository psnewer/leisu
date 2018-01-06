#! /bin/bash

cd `dirname $0`
#
#sh -x run_extract.sh ./conf/extract.txt
#
#python merge_extract_trainall.py
#
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
#
#cd /Users/miller/Documents/workspace/leisu/ZOO
#
#while read LINE
#do
#	{
#		caffe train -solver $LINE
#		checkError "train $LINE"
#	} &
#done  < ./solvers.txt
#
#wait
#echo "train done!!!!"
#
cd /Users/miller/Documents/workspace/leisu/res/extract

for ((i=1; i<=30; i++))
do
    echo >&1000
done

for dir in `ls .`
do
	if [ -d "$dir" ];
	then 
		echo $dir
		cd $dir
		for _dir in `ls .`
		do
			if [ -d "$_dir" ];
			then
				cd $_dir
				if [ -f "VS_RAWALL_EXTRACTOR_extract_test.txt" ];
				then
					caffe test -model /Users/miller/Documents/workspace/leisu/ZOO/leisu_vs_rawall_rt_train.prototxt -weights /Users/miller/Documents/workspace/leisu/ZOO/snap/vs_rawall/_iter_1000000.caffemodel > ./rawall_log.txt 2>&1
				fi
				if [ -f "VS_RAWALL_NO_PRE_EXTRACTOR_extract_test.txt" ];
				then
					caffe test -model /Users/miller/Documents/workspace/leisu/ZOO/leisu_vs_rawall_no_pre_rt_train.prototxt -weights /Users/miller/Documents/workspace/leisu/ZOO/snap/vs_rawall_no_pre/_iter_1000000.caffemodel > ./rawall_no_pre_log.txt 2>&1
				fi
				if [ -f "GOAL_RAWALL_EXTRACTOR_extract_test.txt" ];
				then
					caffe test -model /Users/miller/Documents/workspace/leisu/ZOO/leisu_goal_rt_train.prototxt -weights /Users/miller/Documents/workspace/leisu/ZOO/snap/goal_rawall/_iter_1000000.caffemodel > ./goal_log.txt 2>&1
				fi
				cd ..
			fi
		done
		cd ..
	fi
done

wait
#
#cd /Users/miller/Documents/workspace/leisu/ZOO
#python get_goal_odds.py
#
#cd /Users/miller/Documents/workspace/leisu/res/extract
#
#for dir in `ls .`
#do
#    if [ -d "$dir" ];
#    then
#        echo $dir
#        cd $dir
#        for _dir in `ls .`
#        do
#            if [ -d "$_dir" ];
#            then
#                cd $_dir
#                if [ -f "GOAL_RAWALL_EXTRACTOR_extract_all.txt" ];
#                then
#                read -u1000
#                {
#                    cp /Users/miller/Documents/workspace/leisu/ZOO/leisu_goal_fine_solver.prototxt .
#                    cp /Users/miller/Documents/workspace/leisu/ZOO/leisu_goal_fine_train.prototxt .
#                    cp /Users/miller/Documents/workspace/leisu/ZOO/leisu_goal_fine_deploy.prototxt .
#                    mkdir snap
#                    mkdir snap/goal_rawall_all
#                    caffe train -solver ./leisu_goal_fine_solver.prototxt -weights /Users/miller/Documents/workspace/leisu/ZOO/snap/goal_rawall/_iter_1000000.caffemodel
#                    checkError "train dir $dir $_dir goal"
#                    echo >&1000
#                } &
#                fi
#                cd ..
#            fi
#        done
#        cd ..
#    fi
#done
#
#wait
#
#echo "done!!!"
#
#
