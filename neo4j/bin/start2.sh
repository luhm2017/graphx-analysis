#!/bin/bash
## usage: sh bin/start.sh -i /logs/device/* -d 2016-01-11

SPARK_HOME=/home/hadoop/spark-1.6.3-bin-hadoop2.6
HIVE_HOME=/usr/hdp/current/hive-client
PROJECT_HOME="$(cd "`dirname "$0"`"/..; pwd)"
HDP_VERSION=2.4.0.0-169
APP_CACHE_DIR=/tmp/device

stdate=${1:-`date -d '1 days ago' +"%Y-%m-%d"`}
#inputdir=/logs/device/*
#inputfile=/logs/device/*/2016-01-{1[1-9],2[0-1]}
while getopts "d:i:" opt ; do
 case $opt in
  d)stdate=$OPTARG ;;
  i)inputdir=$OPTARG ;;
  ?)echo "==> please input arg: stdate(d), inputdir(i)" && exit 1 ;;
 esac
done

#echo "==> ready for geoip...."
#hadoop fs -mkdir -p $APP_CACHE_DIR/geoip
#hadoop fs -test -e $APP_CACHE_DIR/geoip/GeoLite2-City.mmdb
#if [ $? -ne 0 ]; then
#    echo "GeoLite2-City.mmdb not exists!"
#    hadoop fs -put $PROJECT_HOME/../tcloud-log-analysis/src/main/bundleApp/coord-common/geoip/GeoLite2-City.mmdb $APP_CACHE_DIR/geoip/
#fi

## https://issues.apache.org/jira/browse/ZEPPELIN-93
## https://github.com/caskdata/cdap/pull/4106
spark-submit \
 --master spark://datacenter17:7077,datacenter18:7077 \
 --class com.lakala.datacenter.main.Driver \
 --driver-memory 2G \
 --executor-memory 4G \
 --num-executors 3 \
 --executor-cores 3 \
 --conf "spark.rpc.askTimeout=300s" \
 --verbose \
 --files $SPARK_HOME/conf/hive-site.xml \
 --driver-class-path $PROJECT_HOME/target/dependency/mysql-connector-java-5.1.36.jar \
 --jars $PROJECT_HOME/target/dependency/mysql-connector-java-5.1.36.jar,$SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$PROJECT_HOME/target/dependency/guava-14.0.1.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar \
 $PROJECT_HOME/target/graphx-analysis-apply.jar \
 -i /user/linyanshi/query_result.csv -c /user/linyanshi/part-00003 -o file:////home/hadoop/grogram/analysis/graphx-analysis/apply/bin/output

## --packages com.databricks:spark-csv_2.10:1.3.0 \
## 2>&1 > output.txt
