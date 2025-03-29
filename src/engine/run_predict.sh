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

cd ../..
scrapy crawl sl -a category=predict
scrapy crawl gj -a category=predict

cd ./src/engine/
python main.py --extract --feature --filter --predict --soccer

