#!/bin/bash

cd `dirname $0`

rm ids.json

echo "{}" > ids.json

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

python matchDB.py
checkError "matchDB"
python oddsDB.py
checkError "oddsDB"

cd /Users/miller/Documents/workspace/leisu/src/engine/script
python get_groupcondition.py
checkError "get_groupcondition"
