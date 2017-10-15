#!/bin/bash

cd `dirname $0`

rm ids.json

echo "{}" > ids.json

python matchDB.py
python oddsDB.py

cd /Users/miller/Documents/workspace/leisu/src/engine/script
python get_groupcondition.py
