-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors (
  number int,
  first_name string,
  last_name string)
STORED AS AVRO;

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/doctors.avro' INTO TABLE doctors;

CREATE TABLE doctors_copy (
  number int,
  first_name string)
STORED AS AVRO;

INSERT INTO TABLE doctors_copy SELECT number, first_name FROM doctors;

ALTER TABLE doctors_copy ADD COLUMNS (last_name string);

LOAD DATA LOCAL INPATH '${hiveconf:test.data.dir}/doctors.avro' INTO TABLE doctors_copy;

DESCRIBE doctors_copy;

SELECT * FROM doctors_copy;
drop table if exists doctors;

drop table if exists doctors_copy;
