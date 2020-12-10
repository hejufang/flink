set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20S)
-- SORT_QUERY_RESULTS

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/T1.txt' INTO TABLE T1;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1 select key, val from T1;

CREATE TABLE outputTbl1(key int, cnt int);

-- The plan should be converted to a map-side group by if the group by key
-- matches the sorted key

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T1 GROUP BY key;

SELECT * FROM outputTbl1;

CREATE TABLE outputTbl2(key1 int, key2 string, cnt int);

-- no map-side group by even if the group by key is a superset of sorted key

INSERT OVERWRITE TABLE outputTbl2
SELECT key, val, count(1) FROM T1 GROUP BY key, val;

SELECT * FROM outputTbl2;

-- It should work for sub-queries

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM (SELECT key, val FROM T1) subq1 GROUP BY key;

SELECT * FROM outputTbl1;

-- It should work for sub-queries with column aliases

INSERT OVERWRITE TABLE outputTbl1
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1) subq1 GROUP BY k;

SELECT * FROM outputTbl1;

CREATE TABLE outputTbl3(key1 int, key2 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant followed
-- by a match to the sorted key

INSERT OVERWRITE TABLE outputTbl3
SELECT 1, key, count(1) FROM T1 GROUP BY 1, key;

SELECT * FROM outputTbl3;

CREATE TABLE outputTbl4(key1 int, key2 int, key3 string, cnt int);

-- no map-side group by if the group by key contains a constant followed by another column

INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T1 GROUP BY key, 1, val;

SELECT * FROM outputTbl4;

-- no map-side group by if the group by key contains a function

INSERT OVERWRITE TABLE outputTbl3
SELECT key, key + 1, count(1) FROM T1 GROUP BY key, key + 1;

SELECT * FROM outputTbl3;

-- it should not matter what follows the group by
-- test various cases

-- group by followed by another group by

INSERT OVERWRITE TABLE outputTbl1
SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
group by key + key;

SELECT * FROM outputTbl1;

-- group by followed by a union

INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) FROM T1 GROUP BY key
  UNION ALL
SELECT key, count(1) FROM T1 GROUP BY key
) subq1;

SELECT * FROM outputTbl1;

-- group by followed by a union where one of the sub-queries is map-side group by

INSERT OVERWRITE TABLE outputTbl1
SELECT * FROM (
SELECT key, count(1) as cnt FROM T1 GROUP BY key
  UNION ALL
SELECT key + key as key, count(1) as cnt FROM T1 GROUP BY key + key
) subq1;

SELECT * FROM outputTbl1;

-- group by followed by a join

INSERT OVERWRITE TABLE outputTbl1
SELECT subq1.key, subq1.cnt+subq2.cnt FROM 
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as cnt FROM T1 GROUP BY key) subq2
ON subq1.key = subq2.key;

SELECT * FROM outputTbl1;

-- group by followed by a join where one of the sub-queries can be performed in the mapper

CREATE TABLE T2(key STRING, val STRING)
CLUSTERED BY (key, val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T2 select key, val from T1;

-- no mapside sort group by if the group by is a prefix of the sorted key

INSERT OVERWRITE TABLE outputTbl1
SELECT key, count(1) FROM T2 GROUP BY key;

SELECT * FROM outputTbl1;

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys

INSERT OVERWRITE TABLE outputTbl4
SELECT key, 1, val, count(1) FROM T2 GROUP BY key, 1, val;

SELECT * FROM outputTbl4;

CREATE TABLE outputTbl5(key1 int, key2 int, key3 string, key4 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys followed by anything

INSERT OVERWRITE TABLE outputTbl5
SELECT key, 1, val, 2, count(1) FROM T2 GROUP BY key, 1, val, 2;

SELECT * FROM outputTbl5;

-- contants from sub-queries should work fine

INSERT OVERWRITE TABLE outputTbl4
SELECT key, constant, val, count(1) from 
(SELECT key, 1 as constant, val from T2)subq
group by key, constant, val;

SELECT * FROM outputTbl4;

-- multiple levels of contants from sub-queries should work fine

INSERT OVERWRITE TABLE outputTbl4
select key, constant3, val, count(1) from
(
SELECT key, constant as constant2, val, 2 as constant3 from 
(SELECT key, 1 as constant, val from T2)subq
)subq2
group by key, constant3, val;

SELECT * FROM outputTbl4;

set hive.map.aggr=true;
set hive.multigroupby.singlereducer=false;
set mapred.reduce.tasks=31;

CREATE TABLE DEST1(key INT, cnt INT);
CREATE TABLE DEST2(key INT, val STRING, cnt INT);

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;

FROM T2
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1;
select * from DEST2;

-- multi-table insert with a sub-query

FROM (select key, val from T2 where key = 8) x
INSERT OVERWRITE TABLE DEST1 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1;
select * from DEST2;

drop table if exists dest1;

drop table if exists dest2;

drop table if exists outputtbl1;

drop table if exists t1;

drop table if exists outputtbl2;

drop table if exists outputtbl3;

drop table if exists outputtbl5;

drop table if exists outputtbl4;

drop table if exists t2;
