#! /bin/bash

cd `dirname $0`

d1=-67

for ((i=2; i<=2; i++))
do
	d2=$d1
	d1=`expr $d1 - 3`
	date_1=`date -v "$d1"d +%Y%m%d`
	date_2=`date -v "$d2"d +%Y%m%d`
	sh -x trend_test.sh ./conf/group.txt $date_1 $date_2
done
