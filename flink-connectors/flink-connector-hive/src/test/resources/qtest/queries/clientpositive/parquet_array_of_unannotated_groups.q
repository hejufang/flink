-- this test creates a Parquet table from a structure with an unannotated
-- repeated structure of (x,y) structs

CREATE TABLE parquet_array_of_unannotated_groups (
    list_of_points ARRAY<STRUCT<x: FLOAT, y: FLOAT>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/UnannotatedListOfGroups.parquet'
OVERWRITE INTO TABLE parquet_array_of_unannotated_groups;

SELECT * FROM parquet_array_of_unannotated_groups;

DROP TABLE parquet_array_of_unannotated_groups;