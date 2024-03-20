create table test (x UInt32, y UInt32) engine=MergeTree order by x;
insert into test values (1, 2), (2, 3);
select * from (select x, y from test group by grouping sets ((x, y), (y))) where y = 2 settings group_by_use_nulls=1, query_plan_filter_push_down=1;

