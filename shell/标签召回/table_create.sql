create table tuijian.tag_style_cid_score
(  
 style string,
 cid_score string
)  
comment 'tag_style_cid_score'  
partitioned by (dt string)
row format delimited  
fields terminated by '\t'  
stored as textfile; 