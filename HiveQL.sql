create table posts (user string, post string, time bigint)
row format delimited
fields terminated by ','
stored as textfile;

show tables;

describe posts;


load data local inpath 'data/user-posts.txt'
overrite into table posts;

hdfs dfs -cat /user/hive/warehouse/posts/user-posts.txt

select count(1) from posts;

select * from post where user="user2";

select * from posts where time<=1343182133839 limit 2;

drop table posts;

exit;

hdfs dfs -ls /user/hive/warehouse/

#join

create table posts_like(user string, post string, like_count int);

insert overwrite table posts_likes
select p.user, p.user, p.post, l.count
from posts p join likes l on (p.user = l.user);

select * from posts_likes limit 10;



select p.*, l.*
from posts p left outer join likes l on (p.user = l.user)
limit 10;

select p.*, l.*
from posts p right outer join likes l on (p.user = l.user)
limit 10;


select p.*, l.*
from posts p full outer join likes l on (p.user = l.user)
limit 10;  


# подсчитать весь текст 

create table docs (line string);

load data inpath 'docs' overwrite into table docs;

create table word_counts as
select word, count(1) as count from 
	(select explode(split(line, '\s')) as word from docs) w
group by word
order by word;


