#CALL explode_table(',');
#CALL explode_table1(',');

drop table if exists bpi_2013.dummylast9;
drop table if exists bpi_2013.dummylast10;
drop table if exists bpi_2013.temp5;

create table dummylast9(id varchar(200),value varchar(200));
insert into dummylast9
select temp.setA,concat_ws(' & ',temp.setA,temp.setB) from temp;

create table dummylast10(id varchar(200),value varchar(200));
insert into dummylast10
select concat_ws(' & ',temp.setA,temp.setB),temp.setB from temp;

call dummy_table3(',');
call dummy_table4(',');

create table temp5(setA varchar(200),setB varchar(200));
insert into temp5
select * from dummylast;

insert into temp5
select * from dummylast1;

insert into temp5(setA,setB)
select event,initial from start,initial where start.event='i';

insert into temp5(setA,setB)
select final,event from start,final where start.event='o';