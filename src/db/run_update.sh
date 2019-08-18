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

python updateDB.py
checkError "updateDB"

cd ../engine/script
python get_groupcondition.py
checkError "get_groupcondition"
#python get_extractorcondition.py
#checkError "get_extractorcondition.py"
