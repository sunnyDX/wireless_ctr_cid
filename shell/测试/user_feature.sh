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

HQL="select uid,client_type,style_clicks,style_atimes,search_style_atimes,type_clicks,type_atimes,search_type_atimes,area_clicks,area_atimes,search_area_atimes,sectionid_clicks from tuijian.wireless_user_all_behavior_v1 where dt='${TODAY}'"
SAVE_TABLE=tuijian.wireless_user_feature_bucketed
#======================================================
#离散化的列名：是否有子特征（0/1）  离散化的分桶规则
#======================================================
#COL_SPLITS="style_clicks:0 3.0,5.0,12.0,32.0 | style_atimes:0 0.0,5.0,36.0,133.0,479.0,1422.0,3384.0,7327.0,21072.0 | search_style_atimes:0 0.0,31.0,409.0,3130.0 | type_clicks:0 2.0,3.0,4.0,7.0,10.0,16.0,27.0,49.0,108.0 | type_atimes:0 0.0,3.0,35.0,146.0,576.0,1754.0,4266.0,9837.6,29473.0 | search_type_atimes:0 0.0,1.0,10.0,39.0,130.0,509.0,1582.0,3687.0,8921.0 | area_clicks:0 2.0,4.0,9.0,24.0 | area_atimes:0 0.0,59.0,620.0,4453.0 | search_area_atimes:0 0.0,25.0,283.0,2706.0 | sectionid_clicks:0 1.0,3.0,17.0"

#======================================================
#离散化的列名：是否有子特征（0/1）: 离散化的分桶个数
#======================================================
COL_SPLITS="style_clicks:0:5,style_atimes:0:10,search_style_atimes:0:5,type_clicks:0:10,type_atimes:0:10,search_type_atimes:0:10,area_clicks:0:5,area_atimes:0:5,search_area_atimes:0:5,sectionid_clicks:0:4"

${SPARK_BIN}  --class ${CLASS_NAME}  --num-executors 30 --driver-memory 6g --executor-memory 8g  --executor-cores 4  --conf spark.sql.shuffle.partitions=300 --conf spark.yarn.queue=datamining ${JAR_FILE} ${HQL} ${TODAY} ${SAVE_TABLE} ${COL_SPLITS}

if [ $? -eq 0 ];then
    echo "user feature encode success"  
else
	echo "user feature encode fail"
	exit 1
fi
