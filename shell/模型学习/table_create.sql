create table tuijian.wireless_user_samples_fact_v3
(  
        label int,
	    style_clicks string,
	    style_atimes string,
	    search_style_atimes string,  
	    type_clicks string,
	    type_atimes string,
	    search_type_atimes string,
	    area_clicks string,
	    area_atimes string,
	    search_area_atimes string,  
		sectionid_clicks string,
		fcid string,
        areaid string,
        styleid string,	  
        typeid string,
        displaycnt string,
        vvcnt string,  
        vvsec string,
        funclickcnt string,
        conclickcnt string
)  
comment 'wireless_user_samples_fact_v3'  
partitioned by (dt string)
row format delimited  
fields terminated by '\t'  
stored as textfile; 
