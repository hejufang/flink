set hive.mapred.mode=nonstrict;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- SORT_QUERY_RESULTS

create table nzhang_part like srcpart;

insert overwrite table nzhang_part partition (ds='2010-08-15', hr) select key, value, hr from srcpart where ds='2008-04-08';

select * from nzhang_part;

insert overwrite table nzhang_part partition (ds='2010-08-15', hr=11) select key, value from srcpart where ds='2008-04-08';

select * from nzhang_part;

insert overwrite table nzhang_part partition (ds='2010-08-15', hr) 
select * from (
    select key, value, hr from srcpart where ds='2008-04-08'
    union all
    select '1' as key, '1' as value, 'file,' as hr from src limit 1) s;

show partitions nzhang_part;

select * from nzhang_part where hr = 'file,';



drop table if exists nzhang_part;
