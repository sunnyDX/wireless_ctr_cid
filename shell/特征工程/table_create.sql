create table tuijian.wireless_user_all_behavior_v1 
(  
   uid string,  
   client_type string,
   style_clicks string,
   style_atimes string,
   search_style_atimes string,  
   type_clicks string,
   type_atimes string,
   search_type_atimes string,
   area_clicks string,
   area_atimes string,
   search_area_atimes string,  
   actor_clicks string,
   actor_atimes string,
   search_actor_atimes string,
   sectionid_clicks string
)  
comment 'wireless_user_all_behavior_v1'  
partitioned by (dt string)
row format delimited  
fields terminated by '\t'  
stored as textfile; 






create table tuijian.wireless_user_feature_bucketed
(  
   uid string,
   client_type string,   
   style_clicks string,
   style_atimes string,
   search_style_atimes string,  
   type_clicks string,
   type_atimes string,
   search_type_atimes string,
   area_clicks string,
   area_atimes string,
   search_area_atimes string,
   sectionid_clicks string
)  
comment 'wireless_user_feature_bucketed'  
partitioned by (dt string)
row format delimited  
fields terminated by '\t'  
stored as textfile; 



create table tuijian.wireless_card_feature_bucketed
(  
   cid string,
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
comment 'wireless_card_feature_bucketed'  
partitioned by (dt string)
row format delimited  
fields terminated by '\t'  
stored as textfile; 
