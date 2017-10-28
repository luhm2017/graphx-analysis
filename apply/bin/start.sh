#!/bin/bash
## usage: sh bin/start.sh -i /logs/device/* -d 2016-01-11

SPARK_HOME=/usr/hdp/current/spark-client
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
 --class RunLoadApplyGraphx3 \
 --master yarn \
 --deploy-mode cluster \
 --queue dc \
 --driver-memory 2G \
 --executor-memory 8G \
 --num-executors 4 \
 --executor-cores 3 \
 --conf "spark.rpc.askTimeout=300s" \
 --driver-java-options "-XX:-UseGCOverheadLimit -Xms2G -Xmx2G -XX:MaxPermSize=2G -Dhdp.version=$HDP_VERSION -Dspark.yarn.am.extraJavaOptions=-Dhdp.version=$HDP_VERSION" \
 --verbose \
 --files $PROJECT_HOME/target/classes/hive-site.xml \
 --driver-class-path $PROJECT_HOME/target/dependency/mysql-connector-java-5.1.36.jar \
 --jars $PROJECT_HOME/target/dependency/mysql-connector-java-5.1.36.jar,$SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar,$SPARK_HOME/lib/datanucleus-core-3.2.10.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar \
 $PROJECT_HOME/target/data-analysis-sdk.jar \
 $stdate

## --packages com.databricks:spark-csv_2.10:1.3.0 \
## 2>&1 > output.txt