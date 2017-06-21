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
CLASS_NAME=feature.EncodeSpark_fs
PROJECT_HOME=/opt/project/wireless_card_ctr
JAR_PATH=${PROJECT_HOME}/test
JAR_FILE=${JAR_PATH}/wireless_ctr_cid.jar


echo "==============================================="
echo "###### EncodeSpark ######"

IFS="@"

HQL="select cid,fcid,areaid,styleid,typeid,displaycnt,vvcnt,vvsec,funclickcnt,conclickcnt from tuijian.wireless_cardrec_ctr_cardfeatures where dt='${TODAY}'"
SAVE_TABLE=tuijian.wireless_card_feature_bucketed
#======================================================
#离散化的列名：是否有子特征（0/1）  离散化的分桶规则
#======================================================
#COL_SPLITS="displaycnt:1 509.0,3519.8,6634.4,10936.0,19643.0,35654.3,66857.7,143097.4,405614.8 | vvcnt:1 31.0,108.0,267.0,522.0,1025.0,2116.0,3907.0,8013.0,23055.0 | vvsec:1 4895.0,28158.0,72855.0,141723.0,288383.0,608113.0,1260515.0,2211014.0,7664826.0 | funclickcnt:1 8.0,33.0,82.0,145.0,233.5,424.0,762.0,1680.0,5068.5 | conclickcnt:1 25.0,94.2,209.1,408.2,801.0,1571.3,2761.1,5623.2,16113.7"

#======================================================
#离散化的列名：是否有子特征（0/1）: 离散化的分桶个数
#======================================================
COL_SPLITS="displaycnt:1:10,vvcnt:1:10,vvsec:1:10,funclickcnt:1:10,conclickcnt:1:10"

${SPARK_BIN}  --class ${CLASS_NAME}  --num-executors 25 --driver-memory 8g --executor-memory 8g  --executor-cores 4  --conf spark.default.parallelism=300 --conf spark.yarn.queue=datamining ${JAR_FILE} ${HQL} ${TODAY} ${SAVE_TABLE} ${COL_SPLITS}

if [ $? -eq 0 ];then
    echo "card feature encode success"  
else
	echo "card feature encode fail"
	exit 1
fi
