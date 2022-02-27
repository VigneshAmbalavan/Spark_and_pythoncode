drop table if exists user_activity cascade;

create table user_activity (user_id int, created_at timestamp);
insert into user_activity
select (random() * 249 + 1)::int, 
       timestamp '2018-12-01' + make_interval(days => (random() * 300 + 1)::int)
from generate_series(1,1000)
;