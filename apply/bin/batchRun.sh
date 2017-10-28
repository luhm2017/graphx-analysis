#!/bin/bash

if [ $# -lt 1 ] ; then
echo "USAGE: $0 begin_date [end_date]"
exit 1;
fi

beginDate=$1
yesterday=$(date --date="1 days ago" '+%Y-%m-%d')
endDate=$yesterday
if [ $# -gt 1 ] ; then
    endDate=$2
fi

beginTime=`date -d $beginDate '+%s'`
yesterdayTime=`date -d $yesterday '+%s'`
endTime=`date -d $endDate '+%s'`
if [ $beginTime -gt $yesterdayTime ] ; then
    echo "begin_date can only be yesterday[$endDate] at the latest"
    exit 1;
fi
if [ $endTime -gt $yesterdayTime ] ; then
    echo "end_date can only be yesterday[$yesterday] at the latest"
    exit 1;
fi
if [ $beginTime -gt $endTime ] ; then
    echo "begin_date can only be end_date[$endDate] at the latest"
    exit 1;
fi

#echo $beginDate
#echo $endDate
currentDate=$beginDate
currentTime=$beginTime

cd "`dirname "$0"`"

while [ $currentTime -le $endTime ]
do
    #echo $currentDate
    sh start.sh $currentDate
    currentDate=`date -d "$currentDate +1 day" '+%Y-%m-%d'`
    currentTime=`date -d $currentDate '+%s'`
done
