#!/bin/sh
if [ $# != 1 ];then
  echo "###### <usage>:partition date, default current date! ######"
  TODAY=`date +%Y-%m-%d`
else
  TODAY=$1
fi

YESTERDAY=`date -d "yesterday $TODAY" +%Y-%m-%d`

HIVE_BIN=/opt/modules/hive/bin/hive

echo "==============================================="
echo "###### tag_style_cid_score to generate ######"

#########################################
# about tag_style_cid_score #
#########################################
$HIVE_BIN   -e "
SET mapreduce.job.queuename=datamining;
alter table  tuijian.tag_style_cid_score drop if exists partition(dt='${TODAY}');
insert overwrite table tuijian.tag_style_cid_score partition(dt='${TODAY}')
select style,
       concat_ws(',',collect_set(cid_score)) as cid_score
	from 
		(select style,
			    concat(cid,':',score) as cid_score
			from		    
				(select style,
					    cid,
					    score,
					    row_number() over (partition by style order by score) rank
			        from 
						(select style,
								cid,
								sum(sorting) as score
							from  
								(select style,
										aid,
										sorting
									from 
										(select trim(style) as style,
												cast(id AS String) as id
											from
											(select id,
												   regexp_replace(style,'\\\\[|\\\\]','') as styles
												from wireless.wireless_albums 
												where dt = '${TODAY}'
												and merge_id=0 
												and trailer=0
												and (type=1 or type=2 or type=3 or type=4)
											)albums
											lateral view explode(split(styles,',')) tmp as style
										where style != ''
										)style_aid
									   join
									   (select aid,
											   type,
											   cast(sorting as int) as sorting 
											from wireless.rank_working_ori
											where dt = '${TODAY}'
										)aid_sorting
										on style_aid.id = aid_sorting.aid
								)style_aid_sorting
								join	
								(select aid,
										cid
									from tuijian.wireless_ctr_aidcidmap
									where dt = '${TODAY}'
								)aid_cid
								on style_aid_sorting.aid =aid_cid.aid
							group by style,
									 cid
						)main
				    group by style,
							 cid,
					         score
				)main1   
			where rank < 10		
			group by style,
					 cid,
					 score
		)main2
	group by style  
"
if [ $? -ne 0 ];then
  echo "###### the generate tag_style_cid_score fail! ######"
  exit 1
else
  echo "###### the generate tag_style_cid_score success! ######"
fi