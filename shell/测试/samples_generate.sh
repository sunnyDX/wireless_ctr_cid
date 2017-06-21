#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi

YESTERDAY=`date -d "yesterday $TODAY" +%Y-%m-%d`
WEEK_BEFORE=`date -d "-7days ${TODAY}" +%Y-%m-%d`
TWODAY_BEFORE=`date -d "-3days ${TODAY}" +%Y-%m-%d`

HIVE_BIN=/opt/modules/hive/bin/hive
echo ${TODAY}
echo ${YESTERDAY}

echo "==============================================="
echo "###### Samples to generate ######"

#########################################
# about wireless_user_samples_fact_v3 #
#########################################
$HIVE_BIN   -e "
SET mapreduce.job.queuename=datamining;
alter table  tuijian.wireless_user_samples_fact_v3 drop if exists partition(dt='${TODAY}');
insert overwrite table tuijian.wireless_user_samples_fact_v3 partition(dt='${TODAY}')
select  label,
	    style_clicks,
	    style_atimes,
	    search_style_atimes,  
	    type_clicks,
	    type_atimes,
	    search_type_atimes,
	    area_clicks,
	    area_atimes,
	    search_area_atimes, 
		sectionid_clicks,
		fcid,                                         
		areaid,
		styleid,	  
		typeid,
		displaycnt,
		vvcnt,  
		vvsec,
		funclickcnt,
		conclickcnt
    from 
		(select uid,
				cid,
				label
			from tuijian.wireless_cardctr_uid_cid_label
			where dt >= '${WEEK_BEFORE}'
		)fact
		join
		(select uid,
				style_clicks,
				style_atimes,
				search_style_atimes,  
				type_clicks,
				type_atimes,
				search_type_atimes,
				area_clicks,
				area_atimes,
				search_area_atimes,
				sectionid_clicks
			from tuijian.wireless_user_feature_bucketed
			where dt= '${TODAY}'
		)user_clicks
		on user_clicks.uid = fact.uid
		join
		(select cid,                                        
				fcid,                                         
				areaid,
				styleid,	  
				typeid,
				displaycnt,
				vvcnt,  
				vvsec,
				funclickcnt,
				conclickcnt   
			from tuijian.wireless_card_feature_bucketed
			where dt = '${TODAY}'
		)card
		on fact.cid = card.cid 
"

$HIVE_BIN  -e "alter table tuijian.wireless_user_samples_fact_v3 drop if exists partition(dt='${TWODAY_BEFORE}');"


if [ $? -ne 0 ];then
  echo "###### the generate samples fail! ######"
  exit 1
else
  echo "###### the generate samples success! ######"
fi