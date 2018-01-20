#!/bin/bash

cd `dirname $0`

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

scrapy crawl sl

cd /Users/miller/Documents/workspace/leisu/src/db
sh -x run_update.sh
checkError "run_update"

cd /Users/miller/Documents/workspace/leisu/src/engine
#sh -x run_train.sh
#checkError "run_train"
#cd /Users/miller/Documents/workspace/leisu/src/engine
sh -x run_group.sh -f ./conf/group.txt -u
#checkError "run_group"
#python run_analysis.py --flagfile=./conf/conf.gflag --predict
#checkError "run_analysis"
