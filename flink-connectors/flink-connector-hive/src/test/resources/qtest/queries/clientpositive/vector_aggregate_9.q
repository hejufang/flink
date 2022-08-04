set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table vectortab2k(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/vectortab2k' OVERWRITE INTO TABLE vectortab2k;

create table vectortab2korc(
            t tinyint,
            si smallint,
            i int,
            b bigint,
            f float,
            d double,
            dc decimal(38,18),
            bo boolean,
            s string,
            s2 string,
            ts timestamp,
            ts2 timestamp,
            dt date)
STORED AS ORC;

INSERT INTO TABLE vectortab2korc SELECT * FROM vectortab2k;

explain vectorization expression
select min(dc), max(dc), sum(dc), avg(dc) from vectortab2korc;

-- SORT_QUERY_RESULTS

select min(dc), max(dc), sum(dc), avg(dc) from vectortab2korc;
drop table if exists vectortab2k;

drop table if exists vectortab2korc;