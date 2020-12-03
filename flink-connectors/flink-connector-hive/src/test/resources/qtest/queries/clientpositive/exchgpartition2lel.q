DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (a int) PARTITIONED BY (d1 int);
CREATE TABLE t2 (a int) PARTITIONED BY (d1 int);
CREATE TABLE t3 (a int) PARTITIONED BY (d1 int, d2 int);
CREATE TABLE t4 (a int) PARTITIONED BY (d1 int, d2 int);
CREATE TABLE t5 (a int) PARTITIONED BY (d1 int, d2 int, d3 int);
CREATE TABLE t6 (a int) PARTITIONED BY (d1 int, d2 int, d3 int);
set hive.mapred.mode=nonstrict;
INSERT OVERWRITE TABLE t1 PARTITION (d1 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT OVERWRITE TABLE t3 PARTITION (d1 = 1, d2 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT OVERWRITE TABLE t5 PARTITION (d1 = 1, d2 = 1, d3=1) SELECT key FROM src where key = 100 limit 1;

SELECT * FROM t1;

SELECT * FROM t3;

ALTER TABLE t2 EXCHANGE PARTITION (d1 = 1) WITH TABLE t1;
SELECT * FROM t1;
SELECT * FROM t2;

ALTER TABLE t4 EXCHANGE PARTITION (d1 = 1, d2 = 1) WITH TABLE t3;
SELECT * FROM t3;
SELECT * FROM t4;

ALTER TABLE t6 EXCHANGE PARTITION (d1 = 1, d2 = 1, d3 = 1) WITH TABLE t5;
SELECT * FROM t5;
SELECT * FROM t6;


drop table if exists t1;

drop table if exists t2;

drop table if exists t3;

drop table if exists t4;

drop table if exists t5;

drop table if exists t6;
