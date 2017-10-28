#!/bin/bash
####################################
## lzo hadoop textfile
## usage:sh lzo.sh source_dir
## example:sh lzo.sh /user/flume
####################################
startTime=`date +%s`
echo "the script begin at $(date +%H:%M:%S)"
source_dir=$1
cd /tmp
hadoop fs -get ${source_dir} /tmp
filepaths=()
function getfilePath(){
    for file in ` ls $1 `
    do
        if [ -d $1"/"$file ]
        then
             getfilePath $1"/"$file
        else
             filepaths[${#filepaths[@]}]=$1"/"$file
        fi
    done
}
path=/tmp/${source_dir##*/}
getfilePath $path
#echo ${filepaths[*]}
for filepath in ${filepaths[@]}
do
        lzop ${filepath}
        rm -rf ${filepath}
done
hadoop fs -mv ${source_dir} ${source_dir}.bak
hadoop fs -put $path ${source_dir%/*}
for filepath in ${filepaths[@]}
do
        hadoop jar /usr/hdp/2.2.6.0-2800/hadoop/lib/hadoop-lzo-0.6.0.2.2.6.0-2800.jar com.hadoop.compression.lzo.LzoIndexer ${source_dir%/*}/${filepath#*/tmp/}.lzo
        #2>&1 > /data/hdfs_logs/${source_dir##*/}.log
done
rm -rf $path
endTime=`date +%s`
echo "the script end at $(date +%H:%M:%S)"
echo "total second is" $(($endTime-$startTime))