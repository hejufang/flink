set hive.stats.autogather=true;

create table tab_decimal(a decimal(35,3));

-- insert some data
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/decimal.txt" INTO TABLE tab_decimal;

select count(*) from tab_decimal;

-- compute statistical summary of data
select compute_stats(a, 18) from tab_decimal;