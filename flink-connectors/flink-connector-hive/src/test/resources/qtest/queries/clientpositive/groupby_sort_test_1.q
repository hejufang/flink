set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T1.txt' INTO TABLE T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 select key, val from T1;

CREATE TABLE outputTbl1(key int, cnt int);

-- The plan should be converted to a map-side group by if the group by key
-- matches the sorted key. However, in test mode, the group by wont be converted.
EXPLAIN
INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

drop table if exists outputtbl1;

drop table if exists t1;
