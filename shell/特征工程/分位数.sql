#用户
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
select percentile(cast(cnt as int),array(0.2,0.4,0.6,0.8))
	from
	(select style_clicks,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select style_clicks 
			from  wireless_user_all_behavior_v1 where dt='2017-06-06' 
			and style_clicks !='-1'
		)a
		lateral view explode(split(style_clicks,',')) tmp as style_cnt
	)main
	where not style like '%-1'
	
style_clicks 		[3.0,5.0,12.0,32.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
select percentile(cast(cnt as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select style_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select style_atimes 
			from  wireless_user_all_behavior_v1 where dt='2017-06-06' 
			and style_atimes != '-1'
		)a
		lateral view explode(split(style_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'
	
style_atimes 	[0.0,5.0,36.0,133.0,479.0,1422.0,3384.0,7327.0,21072.0]
	

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
select percentile(cast(cnt as int),array(0.2,0.4,0.6,0.8))
	from
	(select search_style_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select search_style_atimes 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and search_style_atimes !="-1"
		)a
		lateral view explode(split(search_style_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'

search_style_atimes 		[0.0,31.0,409.0,3130.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select type_clicks,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select type_clicks 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and type_clicks !="-1"
		)a
		lateral view explode(split(type_clicks,',')) tmp as style_cnt
	)main
	where not style like '%-1'

type_clicks 	[2.0,3.0,4.0,7.0,10.0,16.0,27.0,49.0,108.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select type_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select type_atimes 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and type_atimes !="-1"
		)a
		lateral view explode(split(type_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'

type_atimes 		[0.0,3.0,35.0,146.0,576.0,1754.0,4266.0,9837.6,29473.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select search_type_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select search_type_atimes 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and search_type_atimes !="-1"
		)a
		lateral view explode(split(search_type_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'

search_type_atimes 	 [0.0,1.0,10.0,39.0,130.0,509.0,1582.0,3687.0,8921.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.2,0.4,0.6,0.8))
	from
	(select area_clicks,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select area_clicks 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and area_clicks !="-1"
		)a
		lateral view explode(split(area_clicks,',')) tmp as style_cnt
	)main
	where not style like '%-1'

area_clicks     [2.0,4.0,9.0,24.0]

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.2,0.4,0.6,0.8))
	from
	(select area_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select area_atimes 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and area_atimes !="-1"
		)a
		lateral view explode(split(area_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'

area_atimes 		[0.0,59.0,620.0,4453.0]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.2,0.4,0.6,0.8))
	from
	(select search_area_atimes,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select search_area_atimes 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and search_area_atimes !="-1"
		)a
		lateral view explode(split(search_area_atimes,',')) tmp as style_cnt
	)main
	where not style like '%-1'

search_area_atimes 			[0.0,25.0,283.0,2706.0]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cnt as int),array(0.3,0.6,0.9))
	from
	(select sectionid_clicks,
		   split(style_cnt,':')[0] as style,
		   split(style_cnt,':')[1] as cnt
		from
		(select sectionid_clicks 
			from  wireless_user_all_behavior_v1 
			where dt='2017-06-06' 
			and sectionid_clicks !='-1'
		)a
		lateral view explode(split(sectionid_clicks,',')) tmp as style_cnt
	)main
	where not style like '%-1'


sectionid_clicks 		[1.0,3.0,17.0]


#卡片
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cast(displaycnt as double) as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select displaycnt 
		from  wireless_cardrec_ctr_cardfeatures 
		where dt='2017-06-15' 
		and displaycnt !='-1'
	)main


displaycnt 		[509.0,3519.8,6634.4,10936.0,19643.0,35654.3,66857.7,143097.4,405614.8]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cast(vvcnt as double) as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select vvcnt 
		from  wireless_cardrec_ctr_cardfeatures 
		where dt='2017-06-15' 
		and vvcnt !='-1'
	)main


vvcnt 	[31.0,108.0,267.0,522.0,1025.0,2116.0,3907.0,8013.0,23055.0]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cast(vvsec as double) as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select vvsec 
		from  wireless_cardrec_ctr_cardfeatures 
		where dt='2017-06-15' 
		and vvsec !='-1'
	)main


vvsec 		[4895.0,28158.0,72855.0,141723.0,288383.0,608113.0,1260515.0,2211014.0,7664826.0]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cast(funclickcnt as double) as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select funclickcnt 
		from  wireless_cardrec_ctr_cardfeatures 
		where dt='2017-06-15' 
		and funclickcnt !='-1'
	)main


funclickcnt  [8.0,33.0,82.0,145.0,233.5,424.0,762.0,1680.0,5068.5]


#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

select percentile(cast(cast(conclickcnt as double) as int),array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9))
	from
	(select conclickcnt 
		from  wireless_cardrec_ctr_cardfeatures 
		where dt='2017-06-15' 
		and conclickcnt !='-1'
	)main


conclickcnt  [25.0,94.2,209.1,408.2,801.0,1571.3,2761.1,5623.2,16113.7]
