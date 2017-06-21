#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi

YEAR_MONTH=`date -d "0 month ago" +%Y-%m`
DAY=`date -d "$TODAY" +%d` 

HIVE_BIN=/opt/modules/hive/bin/hive
HADOOP_BIN=/opt/modules/hadoop/bin/hadoop
SPARK_BIN=/opt/modules/spark/spark/bin/spark-submit
CLASS_NAME=tag.GenerateCidAid
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/jar
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar
TABLE=tuijian.wireless_ctr_aidcidmap
OUTPUT_FILE=/data2/wireless_card_ctr/cid_aid/${YEAR_MONTH}


echo "==============================================="
echo "###### generate cid_aid_score ######"

if [ ! -d "${OUTPUT_FILE}" ];then
	mkdir "${OUTPUT_FILE}"  
fi  

OUTPUT_FILE_PATH=${OUTPUT_FILE}/cid_aid_${DAY}.json

${SPARK_BIN}  --class ${CLASS_NAME}  --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.default.parallelism=300 --conf spark.yarn.queue=datamining ${JAR_FILE} ${TABLE} ${TODAY} ${OUTPUT_FILE_PATH}

if [ $? -eq 0 ];then
    echo "generate cid_aid_score success"  
else
	echo "generate cid_aid_score fail"
	exit 1
fi
