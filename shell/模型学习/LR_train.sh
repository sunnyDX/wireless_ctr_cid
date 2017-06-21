#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi

YEAR_MONTH=`date -d "0 month ago" +%Y-%m`
DAY=`date -d "$TODAY" +%d` 


HADOOP_BIN=/opt/modules/hadoop/bin/hadoop
SPARK_BIN=/opt/modules/spark/spark/bin/spark-submit
CLASS_NAME=model.LR_Train
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/jar
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar
HDFS_HOME=/datamining/wireless_ctr_cid
INPUT_TRAIN=${HDFS_HOME}/train_set_libsvm_v3
OUTPUT_MODEL=/data2/wireless_card_ctr/model/${YEAR_MONTH}



echo "==============================================="
echo "###### train mode ######"

INPUT_TRAIN_PATH=${INPUT_TRAIN}/dt=${TODAY}

if [ ! -d "${OUTPUT_MODEL}" ];then
	mkdir "${OUTPUT_MODEL}"  
fi  

OUTPUT_MODEL_PATH=${OUTPUT_MODEL}/lr_model_${DAY}.json

${SPARK_BIN}  --class ${CLASS_NAME} --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.sql.shuffle.partitions=500 --conf spark.yarn.queue=datamining ${JAR_FILE} ${INPUT_TRAIN_PATH} ${OUTPUT_MODEL_PATH}


if [ $? -eq 0 ];then
    echo "train success"  
else
	echo "train fail"
	exit 1
fi
