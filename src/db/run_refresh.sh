#!/bin/bash

cd `dirname $0`

rm ids.json

echo "{}" > ids.json

python matchDB.py
