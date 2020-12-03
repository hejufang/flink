set hive.mapred.mode=nonstrict;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- SORT_QUERY_RESULTS

CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
INSERT OVERWRITE TABLE dest1 
SELECT src.key, sum(substr(src.value,5)) 
FROM src
GROUP BY src.key;

INSERT OVERWRITE TABLE dest1 
SELECT src.key, sum(substr(src.value,5)) 
FROM src
GROUP BY src.key;

SELECT dest1.* FROM dest1;


drop table if exists dest1;
