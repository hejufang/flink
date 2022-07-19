-- SORT_QUERY_RESULTS

drop table t1;
drop table t2;


create table t1 as select * from src where key < 10;
create table t2 as select * from src where key < 10;

create table t3(key string, cnt int);
create table t4(value string, cnt int);

explain
from
(select * from t1
 union all
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

from
(select * from t1
 union all
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

select * from t3;
select * from t4;

create table t5(c1 string, cnt int);
create table t6(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

select * from t5;
select * from t6;

drop table t1;
drop table t2;

create table t1 as select * from src where key < 10;
create table t2 as select key, count(1) as cnt from src where key < 10 group by key;

create table t7(c1 string, cnt int);
create table t8(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

select * from t7;
select * from t8;

drop table if exists t1;

drop table if exists t2;

drop table if exists t3;

drop table if exists t4;

drop table if exists t5;

drop table if exists t6;

drop table if exists t7;

drop table if exists t8;