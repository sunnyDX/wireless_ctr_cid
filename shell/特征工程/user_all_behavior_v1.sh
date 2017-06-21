#!/bin/sh
HIVE_BIN=/opt/modules/hive/bin/hive
HADOOP_BIN=/opt/modules/hadoop/bin/hadoop



echo "==============================================="
echo "###### user portrait ######"

if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  today=`date +%Y-%m-%d`
else
  today=$1
fi

yesterday=`date -d "yesterday $today" +%Y-%m-%d`


##########################################
# about wireless_user_all_behavior       #
##########################################
$HIVE_BIN   -e "
SET mapreduce.job.queuename=datamining;
alter table  tuijian.wireless_user_all_behavior_v1 drop if exists partition(dt='$today');
insert overwrite table tuijian.wireless_user_all_behavior_v1 partition(dt='$today')
select style_clicks.uid,
       client_type,
	   30day_online_vv_style_vv_cnt_classification,
       30day_online_vv_style_atime_sec_classification,
	   if (30day_online_vv_search_style_atime_sec_classification is NULL,'-1:-1',30day_online_vv_search_style_atime_sec_classification),
	   30day_online_vv_type_vv_cnt_classification,
	   30day_online_vv_type_atime_sec_classification,
	   if (30day_online_vv_search_type_atime_sec_classification is NULL,'-1:-1',30day_online_vv_search_type_atime_sec_classification),
	   30day_online_vv_area_vv_cnt_classification,
	   30day_online_vv_area_atime_sec_classification,
	   if (30day_online_vv_search_area_atime_sec_classification is NULL,'-1:-1',30day_online_vv_search_area_atime_sec_classification),
	   30day_online_vv_actors_vv_cnt_classification,
	   30day_online_vv_actors_atime_sec_classification,
	   if (30day_online_vv_search_actors_atime_sec_classification is NULL,'-1:-1',30day_online_vv_search_actors_atime_sec_classification),
	   if (7day_online_vv_sectionid_vv_cnt_classification is NULL,'-1:-1',7day_online_vv_sectionid_vv_cnt_classification)
    from    
		(select uid,
		        client_type,
				30day_online_vv_style_vv_cnt_classification
			from tuijian.vv_style_vv_cnt_top10_uid_profile
			where dt = '${today}'
		)style_clicks
        join
		(select uid,
				30day_online_vv_style_atime_sec_classification
			from tuijian.vv_style_atime_sec_top10_uid_profile
			where dt = '${today}'
		)style_atimes
		on style_clicks.uid = style_atimes.uid
		left outer join
		(select uid,
				30day_online_vv_search_style_atime_sec_classification
			from tuijian.vv_search_style_atime_sec_top10_uid_profile
			where dt = '${today}'
		)style_search
		on style_clicks.uid = style_search.uid
		join
		(select uid,
				30day_online_vv_type_vv_cnt_classification
			from tuijian.vv_type_vv_cnt_top10_uid_profile
			where dt = '${today}'
		)type_clicks
		on style_clicks.uid = type_clicks.uid
		join
		(select uid,
				30day_online_vv_type_atime_sec_classification
			from tuijian.vv_type_atime_sec_top10_uid_profile  
            where dt = '${today}'
		)type_atimes
		on style_clicks.uid = type_atimes.uid
		left outer join
		(select uid,
				30day_online_vv_search_type_atime_sec_classification
			from tuijian.vv_search_type_atime_sec_top10_uid_profile
            where dt = '${today}'
		)type_search
		on style_clicks.uid = type_search.uid
		join		
		(select uid,
				30day_online_vv_area_vv_cnt_classification
			from tuijian.vv_area_vv_cnt_top10_uid_profile
            where dt = '${today}'
		)area_clicks
		on style_clicks.uid = area_clicks.uid
		join
		(select uid,
				30day_online_vv_area_atime_sec_classification
			from tuijian.vv_area_atime_sec_top10_uid_profile
            where dt = '${today}'
		)area_atimes
		on style_clicks.uid = area_atimes.uid
		left outer join
		(select uid,
				30day_online_vv_search_area_atime_sec_classification
			from tuijian.vv_search_area_atime_sec_top10_uid_profile
            where dt = '${today}'
		)area_search
		on style_clicks.uid = area_search.uid
		join
		(select uid,
				30day_online_vv_actors_vv_cnt_classification
			from tuijian.vv_actors_vv_cnt_top10_uid_profile
            where dt = '${today}'
		)actor_clicks
		on style_clicks.uid = actor_clicks.uid
		join
		(select uid,
				30day_online_vv_actors_atime_sec_classification
			from tuijian.vv_actors_atime_sec_top10_uid_profile
            where dt = '${today}'
		)actor_atimes
		on style_clicks.uid = actor_atimes.uid
		left outer join
		(select uid,
				30day_online_vv_search_actors_atime_sec_classification
			from tuijian.vv_search_actors_atime_sec_top10_uid_profile
            where dt = '${today}'
		)actor_search
		on style_clicks.uid = actor_search.uid
		left outer join
		(select uid,
				7day_online_vv_sectionid_vv_cnt_classification
			from tuijian.vv_sectionid_vv_cnt_top10_uid_profile
            where dt = '${today}'
		)sectionid_clicks
		on style_clicks.uid = sectionid_clicks.uid
"
if [ $? -ne 0 ];then
  echo "###### the portrait about all fail! ######"
  exit 1
else
  echo "###### the portrait about all success! ######"
fi


