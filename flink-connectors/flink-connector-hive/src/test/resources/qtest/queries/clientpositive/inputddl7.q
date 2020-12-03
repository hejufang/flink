-- test for loading into tables with the correct file format
-- test for loading into partitions with the correct file format


CREATE TABLE T1(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE T1;
SELECT COUNT(1) FROM T1;


CREATE TABLE T2(name STRING) STORED AS SEQUENCEFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.seq' INTO TABLE T2;
SELECT COUNT(1) FROM T2;


CREATE TABLE T3(name STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.txt' INTO TABLE T3 PARTITION (ds='2008-04-09');
SELECT COUNT(1) FROM T3 where T3.ds='2008-04-09';


CREATE TABLE T4(name STRING) PARTITIONED BY(ds STRING) STORED AS SEQUENCEFILE;
LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/kv1.seq' INTO TABLE T4 PARTITION (ds='2008-04-09');
SELECT COUNT(1) FROM T4 where T4.ds='2008-04-09';

DESCRIBE EXTENDED T1;
DESCRIBE EXTENDED T2;
DESCRIBE EXTENDED T3 PARTITION (ds='2008-04-09');
DESCRIBE EXTENDED T4 PARTITION (ds='2008-04-09');







drop table if exists t1;

drop table if exists t2;

drop table if exists t3;

drop table if exists t4;
