#!/bin/bash

PWD=$(cd $(dirname $0); pwd) 
cd $PWD 1> /dev/null 2>&1 

TASKNAME=task6_1_whj
# hadoop client 
HADOOP_HOME=/usr/lib/hadoop-current/bin/hadoop
HADOOP_INPUT_DIR=/user/devel/2020211033wenghongjing/clean_data_10000.csv
HADOOP_OUTPUT_DIR=/user/devel/2020211033wenghongjing/hw6/test10000
echo $HADOOP_HOME 
echo $HADOOP_INPUT_DIR 
echo $HADOOP_OUTPUT_DIR 

hadoop fs -rm -r $HADOOP_OUTPUT_DIR 

hadoop jar /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -D mapred.job.name=$TASKNAME \
    -D mapred.job.priority=NORMAL \
    -D mapred.map.tasks=100 \
    -D mapred.reduce.tasks=10 \
    -D mapred.job.map.capacity=100 \
    -D mapred.job.reduce.capacity=100 \
    -D stream.num.map.output.key.fields=1 \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -D stream.memory.limit=1000 \
    -file mapper.py \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR} \
    -mapper "mapper.py" \
    #-reducer "usr/bin/cat" \
    #-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner 


if [ $? -ne 0 ]; then 
    echo 'error' 
    exit 1 
fi 
$HADOOP_HOME fs -touchz ${HADOOP_OUTPUT_DIR}/done 

exit 0 
