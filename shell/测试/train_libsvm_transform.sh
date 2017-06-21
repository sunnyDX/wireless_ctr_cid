#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi


HIVE_BIN=/opt/modules/hive/bin/hive
HADOOP_BIN=/opt/modules/hadoop/bin/hadoop
SPARK_BIN=/opt/modules/spark/spark/bin/spark-submit
CLASS_NAME=model.Hive2LibsvmSpark
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/test
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar
HDFS_HOME=/datamining/wireless_ctr_cid
TABLE=tuijian.wireless_user_samples_fact_v3
OUTPUT_TRAIN=${HDFS_HOME}/train_set_libsvm_v3


echo "==============================================="
echo "###### libsvm transform ######"

hadoop fs -test -e ${OUTPUT_TRAIN}
if [ $? -ne 0 ]; then
    hadoop fs -mkdir ${OUTPUT_TRAIN}
fi

OUTPUT_TRAIN_PATH=${OUTPUT_TRAIN}/dt=${TODAY}

hadoop fs -ls ${OUTPUT_TRAIN_PATH}
if [ $? -eq 0 ]
then
	hadoop fs -rmr ${OUTPUT_TRAIN_PATH}
fi

${SPARK_BIN}  --class ${CLASS_NAME}  --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.default.parallelism=500 --conf spark.yarn.queue=datamining ${JAR_FILE} ${OUTPUT_TRAIN_PATH} ${TABLE} ${TODAY}

if [ $? -eq 0 ];then
    echo "libsvm transform success"  
else
	echo "libsvm transform fail"
	exit 1
fi
