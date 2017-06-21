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
												where dt = '2017-05-23'
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
											   sorting 
											from wireless.rank_working_ori
											where dt = '2017-05-23'
										)aid_sorting
										on style_aid.id = aid_sorting.aid
								)style_aid_sorting
								join	
								(select aid,
										cid
									from tuijian.wireless_ctr_aidcidmap
									where dt = '2017-05-23'
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
	


