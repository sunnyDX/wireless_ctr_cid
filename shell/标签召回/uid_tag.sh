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
CLASS_NAME=tag.GenerateUserTag
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/jar
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar
TABLE=tuijian.wireless_user_all_behavior_v1


echo "==============================================="
echo "###### generate user_tag_score ######"


${SPARK_BIN}  --class ${CLASS_NAME}  --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.default.parallelism=300 --conf spark.yarn.queue=datamining ${JAR_FILE} ${TABLE} ${TODAY}

if [ $? -eq 0 ];then
    echo "generate user_tag_score success"  
else
	echo "generate user_tag_score fail"
	exit 1
fi
