#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi


HADOOP_BIN=/opt/modules/hadoop/bin/hadoop
SPARK_BIN=/opt/modules/spark/spark/bin/spark-submit
CLASS_NAME=feature.FeatureSelector
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/test
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar
HDFS_HOME=/datamining/wireless_ctr_cid
INPUT_TRAIN=${HDFS_HOME}/train_set_libsvm_v3

OUTPUT_FILE=${JAR_PATH}/selectedFeatures.txt



echo "==============================================="
echo "###### feature select ######"

INPUT_TRAIN_PATH=${INPUT_TRAIN}/dt=${TODAY}


${SPARK_BIN}  --class ${CLASS_NAME} --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.sql.shuffle.partitions=500 --conf spark.yarn.queue=datamining ${JAR_FILE} ${INPUT_TRAIN_PATH} ${OUTPUT_FILE}


if [ $? -eq 0 ];then
    echo "feature select success"  
else
	echo "feature select fail"
	exit 1
fi
