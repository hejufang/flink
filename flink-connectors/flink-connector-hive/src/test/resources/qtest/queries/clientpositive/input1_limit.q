-- SORT_QUERY_RESULTS

CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key < 100 LIMIT 5;

SELECT dest1.* FROM dest1;
SELECT dest2.* FROM dest2;





drop table if exists dest1;

drop table if exists dest2;
