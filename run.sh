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
mv /Users/miller/Documents/workspace/leisu/leisu/matches.json /Users/miller/Documents/workspace/leisu/leisu/tmp.json
scrapy crawl odds
mv /Users/miller/Documents/workspace/leisu/leisu/tmp.json /Users/miller/Documents/workspace/leisu/leisu/matches.json

cd /Users/miller/Documents/workspace/leisu/src/db
sh -x run_refresh.sh
checkError "run_refresh"

cd /Users/miller/Documents/workspace/leisu/src/engine

sh -x run_group.sh ./conf/group.txt
checkError "run_group"
python run_analysis.py --flagfile=./conf/conf.gflag --predict
checkError "run_analysis"
