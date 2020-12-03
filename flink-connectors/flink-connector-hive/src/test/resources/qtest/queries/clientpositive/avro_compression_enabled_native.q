-- verify that new joins bring in correct schemas (including evolved schemas)

CREATE TABLE doctors4 (
  number int,
  first_name string,
  last_name string,
  extra_field string)
STORED AS AVRO;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/doctors.avro' INTO TABLE doctors4;

set hive.exec.compress.output=true;

SELECT count(*) FROM src;
drop table if exists doctors4;
